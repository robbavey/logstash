/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.logstash.common.io;

import java.io.Closeable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.DLQEntry;
import org.logstash.Event;
import org.logstash.Timestamp;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.*;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListSet;

import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.logstash.common.io.RecordIOWriter.RECORD_HEADER_SIZE;

public final class DeadLetterQueueWriter implements Closeable {

    private static final Logger logger = LogManager.getLogger(DeadLetterQueueWriter.class);
    private static final long MAX_SEGMENT_SIZE_BYTES = 10 * 1024 * 1024;

    static final String SEGMENT_FILE_PATTERN = "%d.log";
    static final String LOCK_FILE = ".lock";
    private final long maxSegmentSize;
    private final long maxQueueSize;
    private LongAdder currentQueueSize;
    private final Path queuePath;
    private final FileLock lock;
    private RecordIOWriter currentWriter;
    private int currentSegmentIndex;
    private Timestamp lastEntryTimestamp;
    private boolean open;

    private ConcurrentSkipListSet<Path> segments;

    public DeadLetterQueueWriter(Path queuePath, long maxSegmentSize, long maxQueueSize) throws IOException {
        // ensure path exists, create it otherwise.
        Files.createDirectories(queuePath);
        // check that only one instance of the writer is open in this configured path
        Path lockFilePath = queuePath.resolve(LOCK_FILE);
        boolean isNewlyCreated = lockFilePath.toFile().createNewFile();
        FileChannel channel = FileChannel.open(lockFilePath, StandardOpenOption.WRITE);
        try {
            this.lock = channel.lock();
        } catch (OverlappingFileLockException e) {
            if (isNewlyCreated) {
                logger.warn("Previous Dead Letter Queue Writer was not closed safely.");
            }
            throw new RuntimeException("uh oh, someone else is writing to this dead-letter queue");
        }
        this.queuePath = queuePath;
        this.maxSegmentSize = maxSegmentSize;
        this.maxQueueSize = maxQueueSize;
        this.currentQueueSize = new LongAdder();
        this.currentQueueSize.add(getStartupQueueSize());
        currentSegmentIndex = getSegmentPaths(queuePath)
                .map(s -> s.getFileName().toString().split("\\.")[0])
                .mapToInt(Integer::parseInt)
                .max().orElse(0);
        this.segments = new ConcurrentSkipListSet<>((p1, p2) -> {
            Function<Path, Integer> id = (p) -> Integer.parseInt(p.getFileName().toString().split("\\.")[0]);
            return id.apply(p1).compareTo(id.apply(p2));
        });
        segments.addAll(getSegmentPaths(queuePath).collect(Collectors.toList()));
        this.currentWriter = nextWriter();
        this.lastEntryTimestamp = Timestamp.now();
        this.open = true;
    }

    private boolean deleteSegment(Path segment){
        File segmentFile = segment.toFile();
        long size = segmentFile.length();
        segments.remove(segment);
        if (segment.toFile().delete()){
            currentQueueSize.add(0 - size);
            return true;
        }
        return false;
    }

    static Predicate<Path> segmentPrecedes(Timestamp timestamp) {
        return p -> {
            RecordIOReader reader = null;
            try {
                reader = new RecordIOReader(p);
                byte[] event = reader.seekToNextEventPosition(timestamp, entryTimeFromDLQEntry(), Timestamp::compareTo);
                return event == null;
            } catch (IOException e){
                return false;
            } finally {
                if (reader != null) {
                    try{
                        reader.close();
                    } catch (IOException ignored) {}
                }
            }
        };
    }

    private static Function<byte[], Timestamp> entryTimeFromDLQEntry() {
        return (b) ->
        {
            try {
                return DLQEntry.deserialize(b).getEntryTime();
            } catch (IOException e) {
                return null;
            }
        };
    }

    private Predicate<Path> cumulativeSegmentSizeIsGreaterThan(long maxQueueSize){
        return p -> currentQueueSize.longValue() > maxQueueSize;
    }

    private int deleteWhile(Predicate<Path> predicate) throws Exception {
        int count = 0;
        for (Path segment: segments){
            if (predicate.test(segment)){
                deleteSegment(segment);
                count++;
            }else{
                break;
            }
        }
        return count;
    }


    /**
     * Constructor for Writer that uses defaults
     *
     * @param queuePath the path to the dead letter queue segments directory
     * @throws IOException if the size of the file cannot be determined
     */
    public DeadLetterQueueWriter(String queuePath) throws IOException {
        this(Paths.get(queuePath), MAX_SEGMENT_SIZE_BYTES, Long.MAX_VALUE);
    }

    private long getStartupQueueSize() throws IOException {
        return getSegmentPaths(queuePath)
                .mapToLong((p) -> {
                    try {
                        return Files.size(p);
                    } catch (IOException e) {
                        return 0L;
                    }
                } )
                .sum();
    }

    private RecordIOWriter nextWriter() throws IOException {
        return new RecordIOWriter(newSegment());
    }

    private Path newSegment() {
        Path segmentName = queuePath.resolve(String.format(SEGMENT_FILE_PATTERN, ++currentSegmentIndex));
        this.segments.add(segmentName);
        currentQueueSize.increment();
        return segmentName;
    }

    static Stream<Path> getSegmentPaths(Path path) throws IOException {
        return Files.list(path).filter((p) -> p.toString().endsWith(".log"));
    }

    public synchronized void writeEntry(DLQEntry entry) throws IOException {
        innerWriteEntry(entry);
    }

    public synchronized void writeEntry(Event event, String pluginName, String pluginId, String reason) throws IOException {
        Timestamp entryTimestamp = Timestamp.now();
        if (entryTimestamp.getTime().isBefore(lastEntryTimestamp.getTime())) {
            entryTimestamp = lastEntryTimestamp;
        }
        DLQEntry entry = new DLQEntry(event, pluginName, pluginId, reason);
        innerWriteEntry(entry);
        lastEntryTimestamp = entryTimestamp;
    }

    private void innerWriteEntry(DLQEntry entry) throws IOException {
        byte[] record = entry.serialize();
        int eventPayloadSize = RECORD_HEADER_SIZE + record.length;
        if (currentQueueSize.longValue() + eventPayloadSize > maxQueueSize) {
            logger.error("cannot write event to DLQ: reached maxQueueSize of " + maxQueueSize);
            return;
        } else if (currentWriter.getPosition() + eventPayloadSize > maxSegmentSize) {
            currentWriter.close();
            currentWriter = nextWriter();
        }
        currentQueueSize.add(currentWriter.writeEvent(record));
    }

    @Override
    public synchronized void close() throws IOException {
        this.lock.release();
        if (currentWriter != null) {
            currentWriter.close();
        }
        Files.deleteIfExists(queuePath.resolve(LOCK_FILE));
        open = false;
    }

    public boolean isOpen() {
        return open;
    }

    public Path getPath(){
        return queuePath;
    }

    public long getCurrentQueueSize() {
        return currentQueueSize.longValue();
    }


    /**
     * Deletes segments that only contain entries older than the timestamp given.
     * @param timestamp
     * @return number of segments deleted.
     * @throws Exception
     */
    public int deleteSegmentsOlder(Timestamp timestamp) throws Exception {
        return deleteWhile(segmentPrecedes(timestamp));
    }

    /**
     * Deletes segments over the size of that given.
     * @param size
     * @return number of segments deleted.
     * @throws Exception
     */
    public int deleteSegmentsBigger(long size) throws Exception {
        return deleteWhile(cumulativeSegmentSizeIsGreaterThan(size));
    }


//    private void deleteSegmentsBefore(Path path){
//        segments.headSet(path).forEach(this::deleteSegment);
//    }
//
//    public void deleteSegmentsGreaterThan(long size){
//        for (Path segment: segments){
//            if (currentQueueSize.longValue() < size){
//                return;
//            }
//            if (segment != currentWriter){
//                deleteSegment(segment);
//            }else{
//                return;
//            }
//        }
//    }
//
//    public void deleteSegmentsOlderThan(int retentionPeriod, TimeUnit retentionUnit) throws IOException {
//        DateTime dt = DateTime.now();
//        // Making the simplifying assumption that days does not take DST into account.
//        dt.minus(retentionUnit.toMillis(retentionPeriod));
//        deleteSegmentsBefore(new Timestamp(dt));
//    }
//
//    public void deleteSegmentsBefore(Timestamp timestamp) throws IOException {
//        segmentPreceding(timestamp).ifPresent(this::deleteSegmentsBefore);
//    }
//
//    private Optional<Path> segmentPreceding(Timestamp timestamp) throws IOException {
//        // Refresh segments
//        segments.addAll(getSegmentPaths(queuePath).collect(Collectors.toList()));
//        for (Path segment : segments) {
//
//            RecordIOReader currentReader = new RecordIOReader(segment);
//            byte[] event = currentReader.seekToNextEventPosition(timestamp, (b) -> {
//                try {
//                    return DLQEntry.deserialize(b).getEntryTime();
//                } catch (IOException e) {
//                    return null;
//                }
//            }, Timestamp::compareTo);
//            if (event != null) {
//                return Optional.of(segment);
//            }
//            currentReader.close();
//        }
//        return Optional.empty();
//    }
//
//

//    public boolean deleteOldestSegment() throws IOException, InterruptedException {
//        Optional<Path> oldest = getOldestNonCurrentWriter();
//        if (!oldest.isPresent()){
//            pollNewSegments(200);
//            oldest = getOldestNonCurrentWriter();
//        }
//        return oldest.map(this::deleteSegment).orElse(false);
//    }
}

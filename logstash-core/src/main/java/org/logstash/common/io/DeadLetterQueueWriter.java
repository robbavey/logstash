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
import org.joda.time.DateTime;
import org.logstash.DLQEntry;
import org.logstash.Event;
import org.logstash.Timestamp;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.logstash.common.io.RecordIOWriter.RECORD_HEADER_SIZE;

public final class DeadLetterQueueWriter implements Closeable {

    private static final Logger logger = LogManager.getLogger(DeadLetterQueueWriter.class);
    private static final long MAX_SEGMENT_SIZE_BYTES = 10 * 1024 * 1024;

    static final String SEGMENT_FILE_PATTERN = "%d.log";
    private static final String LOCK_FILE = ".lock";
    private final long maxSegmentSize;
    private final long maxQueueSize;
    private final long maxRetentionMs;
    private LongAdder currentQueueSize;
    private final Path queuePath;
    private final FileLock lock;
    private RecordIOWriter currentWriter;
    private int currentSegmentIndex;
    private Timestamp lastEntryTimestamp;
    private boolean open;
    private ScheduledExecutorService scheduler;

    private ConcurrentSkipListSet<Path> segments;

    public DeadLetterQueueWriter(Path queuePath, long maxSegmentSize, long maxQueueSize, long maxRetentionMs) throws IOException {
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
        this.maxRetentionMs = maxRetentionMs;
        this.currentQueueSize = new LongAdder();
        this.currentQueueSize.add(getStartupQueueSize());
        this.segments = new ConcurrentSkipListSet<>((p1, p2) -> {
            Function<Path, Integer> id = (p) -> Integer.parseInt(p.getFileName().toString().split("\\.")[0]);
            return id.apply(p1).compareTo(id.apply(p2));
        });
        segments.addAll(getSegmentPaths(queuePath).collect(Collectors.toList()));
        currentSegmentIndex = (segments.size() > 0) ? segmentNameToIndex(segments.last()) : 0;
        this.currentWriter = nextWriter();
        this.lastEntryTimestamp = Timestamp.now();
        this.scheduler = new ScheduledThreadPoolExecutor(1);
        scheduler.scheduleAtFixedRate(this::purge, 30, 1, TimeUnit.MINUTES);
        this.open = true;
    }

    /**
     * Constructor for Writer that uses defaults
     *
     * @param queuePath the path to the dead letter queue segments directory
     * @throws IOException if the size of the file cannot be determined
     */
    public DeadLetterQueueWriter(String queuePath) throws IOException {
        this(Paths.get(queuePath), MAX_SEGMENT_SIZE_BYTES, Long.MAX_VALUE, -1);
    }

    public DeadLetterQueueWriter(final DeadLetterQueueSettings settings) throws IOException {
        this(settings.getQueuePath(), settings.getMaxSegmentSize(), settings.getMaxQueueSize(), settings.getMaxRetentionMs());
    }

    private int segmentNameToIndex(Path path){
        return Integer.parseInt(path.getFileName().toString().split("\\.")[0]);
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

    static Stream<Path> getSegmentPaths(Path path) throws IOException {
        return Files.list(path).filter((p) -> p.toString().endsWith(".log"));
    }

    synchronized void writeEntry(DLQEntry entry) throws IOException {
        innerWriteEntry(entry);
    }

    private void purge()  {
        try {
            deleteWhile(cumulativeSegmentSizeIsGreaterThan(maxQueueSize).or(segmentContainsEntriesAfter(timestampFromRetention())));
        } catch (Exception e) {}
    }

    private Timestamp timestampFromRetention(){
        return new Timestamp(DateTime.now().minusMillis((int)maxRetentionMs));
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
        System.out.println("Writing time of entry " + entry.getEntryTime() + " to segment" + currentSegmentIndex);
        currentQueueSize.add(currentWriter.writeEvent(record));
    }

    @Override
    public synchronized void close() throws IOException {
        if (currentWriter != null) {
            currentWriter.close();
        }
        Files.deleteIfExists(queuePath.resolve(LOCK_FILE));
        open = false;
        this.lock.release();
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
     * @param timestamp Delete segments that contain data older than this.
     * @return number of segments deleted.
     * @throws Exception if the operation fails.
     */
    int deleteSegmentsOlderThan(Timestamp timestamp) throws Exception {
        return deleteWhile(segmentContainsEntriesAfter(timestamp));
    }

    int deleteSegmentsOlderThan(int timeInHours) throws Exception {
        return deleteSegmentsOlderThan(new Timestamp(DateTime.now().minusHours(timeInHours)));
    }

    /**
     * Deletes segments over the size of that given.
     * @param size Total size of segments that should not be exceeded.
     * @return number of segments deleted.
     * @throws Exception if the operation fails.
     */
    int deleteSegmentsUntilSmallerThan(long size) throws Exception {
        return deleteWhile(cumulativeSegmentSizeIsGreaterThan(size));
    }

    /**
     * Function to delete segments from the oldest while
     *  1) The predicate holds
     *  2) We have not reached the 'final' segment - ie that being written
     * @param predicate -
     * @return
     * @throws Exception
     */
    private int deleteWhile(Predicate<Path> predicate) throws Exception {
        int count = 0;
        for (Path segment: segments){
            // Don't delete the final segment.
            if (segments.size() == 1){
                break;
            }
            if (predicate.test(segment)){
                if (deleteSegment(segment)) {
                    count++;
                }
            }else{
                break;
            }
        }
        return count;
    }

    private boolean deleteSegment(final Path segment){
        File segmentFile = segment.toFile();
        long size = segmentFile.length();
        if (segmentFile.delete()){
            segments.remove(segment);
            currentQueueSize.add(0 - size);
            return true;
        }
        return false;
    }

    /**
     * Helper function to extract entry time from a serialized DLQ Entry
     * Returns null in the case of failed deserialization
     * @return The timestamp if the DLQ Entry is valid and has a timestamp, null otherwise
     */
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

    /**
     * Predicate to determine if a segment includes the given timestamp.
     * @param timestamp Timestamp of the date
     * @return True if segment contains entries after the timestamp given.
     */
    private Predicate<Path> segmentContainsEntriesAfter(Timestamp timestamp) {
        return p -> {
            try (RecordIOReader reader = new RecordIOReader(p)){
                return null == reader.seekToNextEventPosition(timestamp, entryTimeFromDLQEntry(), Timestamp::compareTo);
            } catch (IOException e) {
                return true;
            }
        };
    }

    private Predicate<Path> cumulativeSegmentSizeIsGreaterThan(long maxQueueSize){
        return p -> currentQueueSize.longValue() > maxQueueSize;
    }

    private Predicate<Path> both(Timestamp x, long y){
        return cumulativeSegmentSizeIsGreaterThan(y).or(segmentContainsEntriesAfter(x));
    }

    private synchronized Path nextSegment() {
        Path segmentName = queuePath.resolve(String.format(SEGMENT_FILE_PATTERN, ++currentSegmentIndex));
        segments.add(segmentName);
        currentQueueSize.increment();
        return segmentName;
    }

    private RecordIOWriter nextWriter() throws IOException {
        return new RecordIOWriter(nextSegment());
    }

}

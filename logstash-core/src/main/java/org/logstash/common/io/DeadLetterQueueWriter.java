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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.jruby.RubyProcess;
import org.logstash.DLQEntry;
import org.logstash.Event;
import org.logstash.Timestamp;

import java.io.Closeable;
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
        this.segments = new ConcurrentSkipListSet<>((p1, p2) -> {
            Function<Path, Integer> id = (p) -> Integer.parseInt(p.getFileName().toString().split("\\.")[0]);
            return id.apply(p1).compareTo(id.apply(p2));
        });
        segments.addAll(getSegmentPaths(queuePath).collect(Collectors.toList()));
        currentSegmentIndex = (segments.size() > 0) ? segmentNameToIndex(segments.last()) : 0;
        this.currentWriter = nextWriter();
        this.lastEntryTimestamp = Timestamp.now();
        this.open = true;
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

    public DeadLetterQueueWriter(final DeadLetterQueueSettings settings) throws IOException {
        this(settings.getQueuePath(), settings.getMaxSegmentSize(), settings.getMaxQueueSize());
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
        try {
            if (currentWriter != null) {
                currentWriter.close();
            }
        }finally {
            Files.deleteIfExists(queuePath.resolve(LOCK_FILE));
            open = false;
            this.lock.release();
        }
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
     * Function to delete segments from the oldest while
     *  1) The predicate holds
     *  2) We have not reached the 'final' segment - ie that being written
     * @param predicate - Predicate function. Implementing predicates /
     * @return Number of segments deleted
     * @throws Exception
     */
     int deleteWhile(final Predicate<Path> predicate) throws Exception {
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
        if (!segmentFile.exists()){
            segments.remove(segment);
        }
        long size = segmentFile.length();
        if (segments.size() > 1 && segmentFile.delete()){
            segments.remove(segment);
            currentQueueSize.add(0 - size);
            return true;
        }
        return false;
    }

    private Path nextSegment() {
        Path segmentName = queuePath.resolve(String.format(SEGMENT_FILE_PATTERN, ++currentSegmentIndex));
        segments.add(segmentName);
        return segmentName;
    }

    private RecordIOWriter nextWriter() throws IOException {
        RecordIOWriter writer = new RecordIOWriter(nextSegment());
        currentQueueSize.add(RecordIOWriter.VERSION_SIZE);
        return writer;
    }
}

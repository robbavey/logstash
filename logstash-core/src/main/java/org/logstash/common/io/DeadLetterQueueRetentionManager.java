package org.logstash.common.io;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.logstash.DLQEntry;
import org.logstash.Timestamp;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

public class DeadLetterQueueRetentionManager  {
    private static final Logger logger = LogManager.getLogger(DeadLetterQueueWriter.class);
    private final long maxRetainedSize;
    private final long maxRetainedMillis;
    private final long cleanIntervalMillis;
    private final ScheduledExecutorService scheduler;
    private final DeadLetterQueueWriter writer;

    public DeadLetterQueueRetentionManager(final DeadLetterQueueSettings settings, final DeadLetterQueueWriter writer) throws IOException{
        this.maxRetainedSize = settings.getMaxRetainedSize();
        this.maxRetainedMillis = settings.getMaxRetainedMillis();
        this.cleanIntervalMillis = settings.getCleanInterval();
        this.writer = writer;
        this.scheduler = new ScheduledThreadPoolExecutor(1);
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::purge, 0, cleanIntervalMillis, TimeUnit.MILLISECONDS);
    }

    public void purge()  {
        try {
            if (maxRetainedSize == -1){
                if (maxRetainedMillis > 0){
                    writer.deleteWhile(segmentContainsEntriesAfter(timestampFromRetention()));
                }
            } else{
                if (maxRetainedMillis == -1) {
                    writer.deleteWhile(cumulativeSegmentSizeIsGreaterThan(maxRetainedSize));
                }else{
                    writer.deleteWhile(cumulativeSegmentSizeIsGreaterThan(maxRetainedSize).or(segmentContainsEntriesAfter(timestampFromRetention())));
                }
            }
        } catch (Exception e) {
            logger.warn("unable to clean dlq", e);
        }
    }

    private Timestamp timestampFromRetention(){
        return new Timestamp(Instant.now().minus(Duration.ofMillis(maxRetainedMillis)).toEpochMilli());
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
    private Predicate<Path> segmentContainsEntriesAfter(final Timestamp timestamp) {;
        return p -> {
            try (RecordIOReader reader = new RecordIOReader(p)){
                return null == reader.seekToNextEventPosition(timestamp, entryTimeFromDLQEntry(), Timestamp::compareTo);
            } catch (IOException e) {
                return false;
            }
        };
    }

    /**
     * Predicate to determine if the cumulative size of all the segments is greater than the given size.
     * @param size Maximum cumulative size of segments.
     * @return True if the total size of segments is larger than the given size
     */
    private Predicate<Path> cumulativeSegmentSizeIsGreaterThan(final long size){
        return p -> writer.getCurrentQueueSize() > size;
    }
}

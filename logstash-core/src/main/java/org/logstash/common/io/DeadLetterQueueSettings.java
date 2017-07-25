package org.logstash.common.io;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

/**
 * This is a work in progress. No effort is made for code reuse/cleanliness
 */
public class DeadLetterQueueSettings {

    private final Path queuePath;
    private final long maxQueueSize;
    private final long maxRetainedSize;
    private final long maxRetainedMillis;
    private final long maxSegmentSize;
    private final String pipelineId;
    private final long cleanInterval;

    public Path getQueuePath(){
        return this.queuePath;
    }

    public long getMaxQueueSize() {
        return this.maxQueueSize;
    }

    public long getMaxRetainedSize() { return this.maxRetainedSize; }

    public long getMaxRetainedMillis() {
        return this.maxRetainedMillis;
    }

    public long getMaxSegmentSize() {
        return this.maxSegmentSize;
    }

    public String getPipelineId() { return this.pipelineId; }

    public long getCleanInterval() { return this.cleanInterval; }

    private DeadLetterQueueSettings(Builder builder){
        this.queuePath = builder.basePath.resolve(builder.pipelineId);
        this.maxQueueSize = builder.maxQueueSize;
        this.maxRetainedSize = builder.maxRetainedSize;
        this.maxRetainedMillis = builder.maxRetainedMillis;
        this.maxSegmentSize = builder.maxSegmentSize;
        this.pipelineId = builder.pipelineId;
        this.cleanInterval = builder.cleanIntervalMillis;
    }

    public static class Builder{
        private String pipelineId;
        private Path basePath;
        private long maxSegmentSize = -1;
        private long maxRetainedSize = -1;
        private long maxQueueSize = -1;
        private long maxRetainedMillis = -1;
        private long cleanIntervalMillis = -1;

        public Builder basePath(String basePath){
            this.basePath = Paths.get(basePath);
            return this;
        }

        public Builder basePath(Path basePath){
            this.basePath = basePath;
            return this;
        }

        public Builder pipelineId(String pipelineId) {
            this.pipelineId = pipelineId;
            return this;
        }

        public Builder maxQueueSize(long maxQueueSize){
            this.maxQueueSize = maxQueueSize;
            return this;
        }

        public Builder maxRetainedSize(long maxRetainedSize){
            this.maxRetainedSize = maxRetainedSize;
            return this;
        }

        public Builder maxRetainedMilliseconds(long maxRetentionMilliseconds){
            this.maxRetainedMillis = maxRetentionMilliseconds;
            return this;
        }

        public Builder maxRetainedTimePeriod(long time, TimeUnit unit){
            this.maxRetainedMillis = unit.toMillis(time);
            return this;
        }

        public Builder maxSegmentSize(long maxSegmentSize) {
            this.maxSegmentSize = maxSegmentSize;
            return this;
        }

        public Builder cleanInterval(long duration, TimeUnit unit){
            this.cleanIntervalMillis = unit.toMillis(duration);
            return this;
        }

        public DeadLetterQueueSettings build(){
            return new DeadLetterQueueSettings(this);
        }
    }

}

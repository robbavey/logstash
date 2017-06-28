package org.logstash.common.io;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * This is a work in progress. No effort is made for code reuse/cleanliness
 */
public class DeadLetterQueueSettings {

    private final Path queuePath;
    private final long maxQueueSize;
    private final long maxRetentionMs;
    private final long maxSegmentSize;
    private final String pipelineId;

    public Path getQueuePath(){
        return this.queuePath;
    }

    public long getMaxQueueSize() {
        return this.maxQueueSize;
    }

    public long getMaxRetentionMs() {
        return this.maxRetentionMs;
    }

    public long getMaxSegmentSize() {
        return this.maxSegmentSize;
    }

    private DeadLetterQueueSettings(Builder builder){
        this.queuePath = Paths.get(builder.basePath, builder.pipelineId);
        this.maxQueueSize = builder.maxQueueSize;
        this.maxRetentionMs = builder.maxRetentionMilliseconds;
        this.maxSegmentSize = builder.maxSegmentSize;
        this.pipelineId = builder.pipelineId;
    }

    public static class Builder{
        private String pipelineId;
        private String basePath;
        private long maxSegmentSize;
        private long maxQueueSize = -1;
        private long maxRetentionMilliseconds = -1;

        public Builder basePath(String basePath){
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

        public Builder maxRetentionMilliseconds(long maxRetentionMilliseconds){
            this.maxRetentionMilliseconds = maxRetentionMilliseconds;
            return this;
        }

        public Builder maxRetention(long time, TimeUnit unit){
            this.maxRetentionMilliseconds = unit.toMillis(time);
            return this;
        }

        public Builder maxSegmentSize(long maxSegmentSize) {
            this.maxSegmentSize = maxSegmentSize;
            return this;
        }

        public DeadLetterQueueSettings build(){
            return new DeadLetterQueueSettings(this);
        }
    }

}

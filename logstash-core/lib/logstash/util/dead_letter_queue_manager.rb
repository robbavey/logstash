require 'logstash/environment'

module LogStash; module Util
  class PluginDeadLetterQueueWriter

    attr_reader :plugin_id, :plugin_type, :inner_writer

    def initialize(inner_writer, plugin_id, plugin_type)
      @plugin_id = plugin_id
      @plugin_type = plugin_type
      @inner_writer = inner_writer
    end

    def write(logstash_event, reason)
      if @inner_writer && @inner_writer.is_open
        @inner_writer.writeEntry(logstash_event.to_java, @plugin_type, @plugin_id, reason)
      end
    end

    def close
      if @inner_writer && @inner_writer.is_open
        @inner_writer.close
      end
    end
  end

  class DummyDeadLetterQueueWriter
    # class uses to represent a writer when dead_letter_queue is disabled
    def initialize
    end

    def write(logstash_event, reason)
      # noop
    end

    def is_open
      false
    end

    def close
      # noop
    end
  end

  class DeadLetterQueueFactory
    java_import org.logstash.common.DeadLetterQueueFactory
    java_import org.logstash.common.io.DeadLetterQueueSettings

    def self.get(pipeline_id)
      if LogStash::SETTINGS.get("dead_letter_queue.enable")
        settings_builder = DeadLetterQueueSettings.Builder.new
        settings = settings_builder.base_path(LogStash::SETTINGS.get("path.dead_letter_queue"))
                         .pipeline_id(pipeline_id)
                         .max_segment_size(1024768)
                         .max_retention_milliseconds(100000000)
                         .max_queue_size(10000000)
                         .build
        # return DeadLetterQueueFactory.getWriter(pipeline_id, LogStash::SETTINGS.get("path.dead_letter_queue"))
        return DeadLetterQueueFactory.get_writer(pipeline_id, settings)
      else
        return DummyDeadLetterQueueWriter.new
      end
    end

    def self.close(pipeline_id)
      DeadLetterQueueFactory.close(pipeline_id)
    end
  end
end end

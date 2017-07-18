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
    java_import import java.util.concurrent.TimeUnit
    java_import org.logstash.common.DeadLetterQueueFactory
    java_import org.logstash.common.io.DeadLetterQueueSettings

    def self.get(settings, pipeline_id)
      if settings.get("dead_letter_queue.enable")
        settings_builder = DeadLetterQueueSettings::Builder.new
        dlq_settings = settings_builder.base_path(settings.get("path.dead_letter_queue"))
                         .pipeline_id(pipeline_id)
                         .max_segment_size(settings.get('dead_letter_queue.segment_max_bytes'))
                         .max_retained_time_period(settings.get('dead_letter_queue.age.threshold'), TimeUnit::NANOSECONDS)
                         .max_retained_size(settings.get('dead_letter_queue.size.threshold'))
                         .max_queue_size(settings.get('dead_letter_queue.max_bytes'))
                         .build
        return DeadLetterQueueFactory.get_writer(dlq_settings)
      else
        return DummyDeadLetterQueueWriter.new
      end
    end

    def self.get_retention_manager(settings, writer)

    end

    def self.close(pipeline_id)
      DeadLetterQueueFactory.close(pipeline_id)
    end
  end
end end

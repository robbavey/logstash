# require "jruby_acked_queue_ext"
# require "jruby_acked_batch_ext"
require "concurrent"

module LogStash; module Util

# This class pushes events straight to the filters, and then the outputs. This
class WrappedDirectQueue
  include LogStash::Util::Loggable
  java_import java.util.Arrays
  java_import java.util.ArrayList
  java_import java.util.concurrent.CompletableFuture
  java_import java.util.concurrent.Executors
  java_import java.util.concurrent.atomic.LongAdder
  def initialize (pipeline)
    @e2e_warned = Concurrent::AtomicBoolean.new(false)
    @pipeline = pipeline
    @execution_map = Concurrent::Map.new
    @sent_items = LongAdder.new
  end

  attr_reader :queue, :pipeline, :execution

  # Push an object to the queue if the queue is full
  # it will block until the object can be added to the queue.
  #
  # @param [obj] Object to add to the queue
  def push(obj)
    logger.warn "Pipeline contains inputs that do not support end to end execution. Please change pipeline type for better performance" if @e2e_warned.make_true
    @pipeline.process_batch([obj], get_execution)
  end
  alias_method(:<<, :push)

  def push_batch(batch)
    @sent_items.add(batch.size)
    @pipeline.process_batch(batch, get_execution)
  end

  def get_execution
      @execution_map.compute_if_absent(Thread.current) { @pipeline.build_execution }
  end

# not implementing
  # def poll(millis)
  # end

  def write_client
    WriteClient.new(self)
  end

  def read_client
    ReadClient.new(self)
  end

  def close
    # ignore
  end

  class ReadClient
    # We generally only want one thread at a time able to access pop/take/poll operations
    # from this queue. We also depend on this to be able to block consumers while we snapshot
    # in-flight buffers

    def initialize(queue, batch_size = 125, wait_for = 250)
      @queue = queue
      # # Note that @inflight_batches as a central mechanism for tracking inflight
      # # batches will fail if we have multiple read clients in the pipeline.
      @inflight_batches = Concurrent::Map.new
      #
      # # allow the worker thread to report the execution time of the filter + output
      @inflight_clocks = Concurrent::Map.new
      # @batch_size = batch_size
      # @wait_for = TimeUnit::NANOSECONDS.convert(wait_for, TimeUnit::MILLISECONDS)
      @wait_for = wait_for / 1000000
    end

    def queued?
      false
    end

    def close
      # noop, compat with acked queue read client
    end

    # def empty?
    #   @queue.queue.isEmpty
    # end

    # def set_batch_dimensions(batch_size, wait_for)
    #   @batch_size = batch_size
    #   @wait_for = wait_for / 1000000
    #   # @wait_for = TimeUnit::NANOSECONDS.convert(wait_for, TimeUnit::MILLISECONDS)
    # end

    def set_events_metric(metric)
      @event_metric = metric
      @event_metric_out = @event_metric.counter(:out)
      @event_metric_filtered = @event_metric.counter(:filtered)
      @event_metric_time = @event_metric.counter(:duration_in_millis)
      define_initial_metrics_values(@event_metric)
    end

    def set_pipeline_metric(metric)
      @pipeline_metric = metric
      @pipeline_metric_out = @pipeline_metric.counter(:out)
      @pipeline_metric_filtered = @pipeline_metric.counter(:filtered)
      @pipeline_metric_time = @pipeline_metric.counter(:duration_in_millis)
      define_initial_metrics_values(@pipeline_metric)
    end

    def define_initial_metrics_values(namespaced_metric)
      namespaced_metric.report_time(:duration_in_millis, 0)
      namespaced_metric.increment(:filtered, 0)
      namespaced_metric.increment(:out, 0)
    end

    def inflight_batches
      # yield(@inflight_batches)
    end
    #
    def current_inflight_batch
      @inflight_batches.fetch(Thread.current, [])
    end
    #
    # # create a new empty batch
    # # @return [ReadBatch] a new empty read batch
    # def new_batch
    #   ReadBatch.new(@queue, 0, 0)
    # end
    #
    # def read_batch
    #   batch = ReadBatch.new(@queue, @batch_size, @wait_for)
    #   start_metrics(batch)
    #   batch
    # end

    def start_metrics(batch)
      set_current_thread_inflight_batch(batch)
      start_clock
    end

    def set_current_thread_inflight_batch(batch)
      @inflight_batches[Thread.current] = batch
    end

    def close_batch(batch)
      @inflight_batches.delete(Thread.current)
      stop_clock(batch)
    end

    def start_clock
      @inflight_clocks[Thread.current] = java.lang.System.nano_time
    end

    def stop_clock(batch)
      start_time = @inflight_clocks.get_and_set(Thread.current, nil)
      unless start_time.nil?
        if batch.size > 0
          # only stop (which also records) the metrics if the batch is non-empty.
          # start_clock is now called at empty batch creation and an empty batch could
          # stay empty all the way down to the close_batch call.
          time_taken = (java.lang.System.nano_time - start_time) / 1_000_000
          @event_metric_time.increment(time_taken)
          @pipeline_metric_time.increment(time_taken)
        end
      end
    end

    def add_filtered_metrics(filtered_size)
      @event_metric_filtered.increment(filtered_size)
      @pipeline_metric_filtered.increment(filtered_size)
    end

    def add_output_metrics(filtered_size)
      @event_metric_out.increment(filtered_size)
      @pipeline_metric_out.increment(filtered_size)
    end
  end

  # class ReadBatch
  #   include LogStash::Util::Loggable
  #   def initialize(queue, size, wait)
  #     logger.warn "Initializing read batch with queue: #{queue}, size: #{size}, wait:#{wait}"
  #     # TODO: disabled for https://github.com/elastic/logstash/issues/6055 - will have to properly refactor
  #     # @cancelled = Hash.new
  #
  #     #Sizing HashSet to size/load_factor to ensure no rehashing
  #     # @originals = LsQueueUtils.drain(queue.queue, size, wait)
  #   end
  #
  #   def merge(event)
  #     # puts " I am merging an evnt"
  #     return if event.nil?
  #     # @originals.add(event)
  #   end
  #
  #   # def cancel(event)
  #   #   # TODO: disabled for https://github.com/elastic/logstash/issues/6055 - will have to properly refactor
  #   #   raise("cancel is unsupported")
  #   #   # @cancelled[event] = true
  #   # end
  #
  #   # def to_a
  #   #   events = []
  #   #   # @originals.each {|e| events << e unless e.cancelled?}
  #   #   events
  #   # end
  #
  #   # def each(&blk)
  #   #   # below the checks for @cancelled.include?(e) have been replaced by e.cancelled?
  #   #   # TODO: for https://github.com/elastic/logstash/issues/6055 = will have to properly refactor
  #   #   # @originals.each {|e| blk.call(e) unless e.cancelled?}
  #   #
  #   # end
  #
  #   # def filtered_size
  #   #   # @originals.size
  #   # end
  #   #
  #   # alias_method(:size, :filtered_size)
  #
  #   # def cancelled_size
  #   #   # TODO: disabled for https://github.com/elastic/logstash/issues/6055 = will have to properly refactor
  #   #   raise("cancelled_size is unsupported ")
  #   #   # @cancelled.size
  #   # end
  # end

  class NamedThreadFactory
    java_import java.util.concurrent.ThreadFactory
    include ThreadFactory

    def initialize(name)
      @name = name

      @@counter = Concurrent::AtomicFixnum.new(0)
    end

    def newThread(runnable)
      thread = java.lang.Thread.new(runnable)
      thread.name = "#{@name}-#{SecureRandom.hex(10)}-#{@@counter.increment}"
      thread
    end
  end

  class WriteClient
    include LogStash::Util::Loggable
    def initialize(queue)
      @queue = queue
      @pipeline = queue.pipeline
      @executor_service = Executors.new_fixed_thread_pool(@pipeline.settings.get_default("pipeline.workers"), NamedThreadFactory.new("e2e-worker"))
      @batch_id = LongAdder.new
    end

    def push(event)
      @queue.push(event)
    end
    alias_method(:<<, :push)


    # what I am trying to do:
    #  Create a pool of workers. Once the workers complete an item of work, pull the next item of work and do it.
    #  I can push this to a work queue. Pulling the next item off the work queue on completion
    #  A pool of workers, when complete get the next one, and write it.

    # Using the worker loop. The worker loop gets a synchronous task, rather than events?

    # I wonder if we can map read batch to a list of

    def push_batch(batch)
      @batch_id.increment
      completable_futures = []
      batch.each_slice(@pipeline.settings.get("pipeline.batch.size")) do |slice|
        completable_futures << CompletableFuture.async_supply_stage(@executor_service) { @queue.push_batch(slice) }
      end
      completable_futures.to_java CompletableFuture
      CompletableFuture.all_of(completable_futures.to_java CompletableFuture).get
      logger.debug "completed batch of #{batch.size}"
    end

  end
end; end; end

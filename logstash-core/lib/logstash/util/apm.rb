module LogStash; module Util; module Apm
  extend self

  require 'java'

  java_import 'io.opentelemetry.api.GlobalOpenTelemetry'
  java_import 'io.opentelemetry.api.trace.Span'
  java_import 'io.opentelemetry.api.trace.Tracer'
  java_import 'io.opentelemetry.context.Context'
  java_import 'io.opentelemetry.context.propagation.TextMapPropagator'
  java_import 'io.opentelemetry.context.propagation.TextMapSetter'

  def with_span(name)
    begin
        puts "Measuring #{name}"
        parent_span = Java::io.opentelemetry.api.trace.Span.current
        puts "the parent span is #{parent_span}"
        span = Java::co.elastic.logstash.api.APM.startTrace(name)
        scope = span.make_current
#       parent_span = Java::co.elastic.apm.api.ElasticApm.currentSpan
#       span = parent_span.startSpan
#       span.setName(name)
#       scope = span.activate
      yield
    ensure
        puts "the scope is #{scope}, the span is #{span}"
        scope.close unless scope.nil?
        span.end unless span.nil?
        puts "Measured #{name}"
#       scope.close
#       span.end
    end
  end

  def add_traceparent(hash)
      propagator = Java::co.elastic.logstash.api.APM.getPropagator
      propagator.inject(Java::io.opentelemetry.context.Context.current(), hash, lambda { |carrier, key, value| carrier.put(key, value) })
      puts "hash is #{hash}"
      hash
  end

end;end;end
module LogStash; module Util; module Apm
  extend self

  def with_span(name)
    begin
      parent_span = Java::co.elastic.apm.api.ElasticApm.currentSpan
      span = parent_span.startSpan
      span.setName(name)
      scope = span.activate
      yield
    ensure
      scope.close
      span.end
    end
  end
end;end;end
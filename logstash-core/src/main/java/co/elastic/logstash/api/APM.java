package co.elastic.logstash.api;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class APM {

    private static OpenTelemetry openTelemetry;
    private static Tracer tracer;
    private static Meter meter;
    private final Map<String, Context> spans = new ConcurrentHashMap<>();
    private static LongCounter counter;
    private static TextMapPropagator propagator;

    public static void setup(){
        openTelemetry = GlobalOpenTelemetry.get();
        tracer = openTelemetry.tracerBuilder("logstash")
                .setInstrumentationVersion("unknown")
                .setSchemaUrl("https://opentelemetry.io/schemas/1.21.0")
                .build();
        meter = GlobalOpenTelemetry.getMeter("my_meter");
        counter = meter.counterBuilder("my_counter").build();
        propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
//        tracer = openTelemetry.getTracer("logstash", "8.14.0");
    }

    public static OpenTelemetry getOTel(){
        return openTelemetry;
    }

    public static Tracer getTracer(){
        return tracer;
    }

    public static TextMapPropagator getPropagator(){
        return propagator;
    }

    public static Span startTrace(String name){
        var spanBuilder = tracer.spanBuilder(name);
        spanBuilder.setSpanKind(SpanKind.INTERNAL);
        return spanBuilder.startSpan();
    }

    public static void stopTrace(Span span){
        span.end();
    }

}

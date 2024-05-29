package co.elastic.logstash.api;

import org.logstash.ConvertedMap;


public final class ConvertedMapSetter
        implements io.opentelemetry.context.propagation.TextMapSetter<ConvertedMap> {
    private ConvertedMapSetter() {}
    public static ConvertedMapSetter SETTER_INSTANCE = new ConvertedMapSetter();

    @Override
    public void set(ConvertedMap carrier, String key, String value) {
        carrier.put(key, value);
    }
}

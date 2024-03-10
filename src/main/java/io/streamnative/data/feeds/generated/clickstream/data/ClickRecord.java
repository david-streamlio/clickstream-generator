package io.streamnative.data.feeds.generated.clickstream.data;

import com.google.gson.Gson;
import io.streamnative.data.feeds.generated.clickstream.data.Click;
import org.apache.pulsar.functions.api.Record;

import java.util.Optional;

public class ClickRecord implements Record<String> {

    private static final Gson gson = new Gson();
    private Click click;

    public ClickRecord(Click c) {
        this.click = c;
    }

    @Override
    public String getValue() {
        return (click != null) ? gson.toJson(click) : null;
    }

    @Override
    public Optional<String> getKey() {
        return Optional.of("");
    }
}

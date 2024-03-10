package io.streamnative.data.feeds.generated.clickstream.data;

import java.io.Closeable;

public interface ClickGenerator extends Closeable {
    public void generate();
}

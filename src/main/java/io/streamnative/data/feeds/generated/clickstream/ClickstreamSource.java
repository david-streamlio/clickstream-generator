package io.streamnative.data.feeds.generated.clickstream;

import io.streamnative.data.feeds.generated.clickstream.data.ClickGenerator;
import io.streamnative.data.feeds.generated.clickstream.data.FileBasedClickGenerator;
import org.apache.pulsar.io.core.PushSource;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;

import java.util.Map;

public class ClickstreamSource extends PushSource<String> {

    private static Logger LOG;

    private ClickGenerator clickGenerator;


    @Override
    public void open(Map<String, Object> config, SourceContext ctx) throws Exception {
        LOG = ctx.getLogger();
        LOG.info("Opening the Click Stream Source.....");

        this.clickGenerator = new FileBasedClickGenerator(this);
        new Thread(() -> { this.clickGenerator.generate(); }).start();
    }

    @Override
    public int getQueueLength() {
        return 5000;
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing the Click Stream Source.....");
    }
}

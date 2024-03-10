package io.streamnative.data.feeds.generated.clickstream;

import org.apache.pulsar.common.functions.FunctionConfig;
import org.apache.pulsar.common.io.SourceConfig;
import org.apache.pulsar.functions.LocalRunner;

public class ClickstreamSourceLocalRunner {

    public static void main(String[] args) throws Exception {

        SourceConfig sourceConfig =
                SourceConfig.builder()
                        .className(ClickstreamSource.class.getName())
                        .name("click-simulator")
                        .topicName("persistent://public/default/clicks-in")
                        .processingGuarantees(FunctionConfig.ProcessingGuarantees.ATMOST_ONCE)
                        .schemaType("string")
                        .build();

        LocalRunner localRunner =
                LocalRunner.builder()
                        .brokerServiceUrl("pulsar://192.168.1.100:6650")
                        .sourceConfig(sourceConfig)
                        .build();

        localRunner.start(false);
        Thread.sleep(120 * 1000);
        localRunner.stop();
    }
}

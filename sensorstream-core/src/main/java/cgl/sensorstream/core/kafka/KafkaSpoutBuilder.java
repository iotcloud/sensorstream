package cgl.sensorstream.core.kafka;

import backtype.storm.topology.IRichSpout;
import cgl.sensorstream.core.ComponentConfiguration;
import cgl.sensorstream.core.SpoutBuilder;
import com.ss.kafka.KafkaSpout;

public class KafkaSpoutBuilder implements SpoutBuilder {
    @Override
    public IRichSpout build(ComponentConfiguration configuration) {
        KafkaSpoutConfigurator configurator = new KafkaSpoutConfigurator(configuration);
        return new KafkaSpout(configurator);
    }
}

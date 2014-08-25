package cgl.sensorstream.core.kafka;

import backtype.storm.topology.IRichBolt;
import cgl.sensorstream.core.BoltBuilder;
import cgl.sensorstream.core.ComponentConfiguration;
import com.ss.kafka.KafkaBolt;

public class KafkaBoltBuilder implements BoltBuilder {
    @Override
    public IRichBolt build(ComponentConfiguration configuration) {
        KafkaBoltConfigurator configurator = new KafkaBoltConfigurator(configuration);
        return new KafkaBolt(configurator);
    }
}

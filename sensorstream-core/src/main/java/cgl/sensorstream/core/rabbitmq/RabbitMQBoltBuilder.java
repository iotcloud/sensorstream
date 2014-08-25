package cgl.sensorstream.core.rabbitmq;

import backtype.storm.topology.IRichBolt;
import cgl.sensorstream.core.BoltBuilder;
import cgl.sensorstream.core.ComponentConfiguration;
import com.ss.rabbitmq.ErrorReporter;
import com.ss.rabbitmq.bolt.RabbitMQBolt;

import java.util.List;
import java.util.Map;

public class RabbitMQBoltBuilder implements BoltBuilder {
    @Override
    public IRichBolt build(ComponentConfiguration configuration) {
        RabbitMQBoltConfigurator boltConfigurator = new RabbitMQBoltConfigurator(configuration);
        ErrorReporter reporter = new ErrorReporter() {
            @Override
            public void reportError(Throwable throwable) {

            }
        };
        return new RabbitMQBolt(boltConfigurator, reporter);
    }
}

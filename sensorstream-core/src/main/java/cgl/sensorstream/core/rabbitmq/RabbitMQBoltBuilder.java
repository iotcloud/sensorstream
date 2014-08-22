package cgl.sensorstream.core.rabbitmq;

import backtype.storm.topology.IRichBolt;
import cgl.sensorstream.core.BoltBuilder;
import com.ss.rabbitmq.ErrorReporter;
import com.ss.rabbitmq.bolt.RabbitMQBolt;

import java.util.List;
import java.util.Map;

public class RabbitMQBoltBuilder implements BoltBuilder {
    @Override
    public IRichBolt build(String toplogyName, String sensor, String channel, List<String> fields, String convertor, Map<String, Object> properties, String zkConnection) {
        RabbitMQBoltConfigurator boltConfigurator = new RabbitMQBoltConfigurator(toplogyName, sensor, channel, convertor, fields, 64, zkConnection);
        ErrorReporter reporter = new ErrorReporter() {
            @Override
            public void reportError(Throwable throwable) {

            }
        };
        return new RabbitMQBolt(boltConfigurator, reporter);
    }
}

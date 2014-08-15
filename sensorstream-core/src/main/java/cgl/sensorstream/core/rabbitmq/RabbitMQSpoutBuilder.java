package cgl.sensorstream.core.rabbitmq;

import backtype.storm.topology.IRichSpout;
import cgl.sensorstream.core.SpoutBuilder;
import com.ss.rabbitmq.ErrorReporter;
import com.ss.rabbitmq.RabbitMQSpout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class RabbitMQSpoutBuilder implements SpoutBuilder {
    private static Logger LOG = LoggerFactory.getLogger(RabbitMQSpoutBuilder.class);

    public IRichSpout build(String sensor, String channel, List<String> fields, String convertor, Map<String, Object> properties, String zkConnection) {
        RabbitMQSpoutConfigurator configurator = new RabbitMQSpoutConfigurator(sensor, channel, fields, convertor, 64, zkConnection);
        ErrorReporter reporter = new ErrorReporter() {
            @Override
            public void reportError(Throwable throwable) {
                LOG.error("error occured", throwable);
            }
        };
        return new RabbitMQSpout(configurator, reporter);
    }
}

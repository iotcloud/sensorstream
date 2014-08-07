package cgl.sensorstream.core.rabbitmq;

import backtype.storm.spout.ISpout;
import cgl.sensorstream.core.SpoutCreator;

import java.util.List;
import java.util.Map;

public class RabbitMQSpoutCreator implements SpoutCreator {
    @Override
    public ISpout create(String url, List<String> fields, Object convertor, Map<String, String> properties) {
        new RabbitMQSpoutConfigurator(url, ,convertor, fields);
        return null;
    }
}

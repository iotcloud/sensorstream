package cgl.sensorstream.core.rabbitmq;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import cgl.sensorstream.core.Utils;
import cgl.sensorstream.core.ZKDestinationChanger;
import com.ss.commons.DestinationChanger;
import com.ss.commons.MessageBuilder;
import com.ss.commons.SpoutConfigurator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RabbitMQSpoutConfigurator implements SpoutConfigurator {
    private int queueSize = 64;

    private String messageBuilder;

    private List<String> fields;

    private String sensor;

    private String channel;

    private String zkConnectionString;

    public RabbitMQSpoutConfigurator(String sensor, String channel,
                                     List<String> fields, String messageBuilder,
                                     int queueSize, String zkConnectionString) {
        this.sensor = sensor;
        this.channel = channel;
        this.messageBuilder = messageBuilder;
        this.fields = fields;
        this.queueSize = queueSize;
        this.zkConnectionString = zkConnectionString;
    }

    @Override
    public MessageBuilder getMessageBuilder() {
        if (messageBuilder != null) {
            return (MessageBuilder) Utils.loadMessageBuilder(messageBuilder);
        } else {
            return new DefaultRabbitMQMessageBuilder();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(fields));
    }

    @Override
    public int queueSize() {
        return queueSize;
    }

    @Override
    public Map<String, String> getProperties() {
        return new HashMap<String, String>();
    }

    @Override
    public DestinationChanger getDestinationChanger() {
        return new ZKDestinationChanger(sensor, channel, zkConnectionString);
    }
}

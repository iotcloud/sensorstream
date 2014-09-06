package cgl.sensorstream.core.kafka;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import cgl.sensorstream.core.ComponentConfiguration;
import cgl.sensorstream.core.Utils;
import cgl.sensorstream.core.ZKDestinationChanger;
import cgl.sensorstream.core.rabbitmq.DefaultRabbitMQMessageBuilder;
import com.ss.commons.BoltConfigurator;
import com.ss.commons.DestinationChanger;
import com.ss.commons.DestinationSelector;
import com.ss.commons.MessageBuilder;

import java.util.List;
import java.util.Map;

public class KafkaBoltConfigurator implements BoltConfigurator {
    private int queueSize = 64;

    private String messageBuilder;

    private List<String> fields;

    private String sensor;

    private String channel;

    private String zkConnectionString;

    private String topologyName;

    private ComponentConfiguration configuration;

    public KafkaBoltConfigurator(ComponentConfiguration configuration) {
        this.topologyName = configuration.getTopologyConfiguration().getTopologyName();
        this.sensor = configuration.getSensor();
        this.channel = configuration.getChannel();
        this.messageBuilder = configuration.getMessageBuilder();
        this.fields = configuration.getFields();
        this.queueSize = configuration.getQueueSize();
        this.zkConnectionString = configuration.getTopologyConfiguration().getZkConnectionString();
        this.configuration = configuration;
    }

    @Override
    public MessageBuilder getMessageBuilder() {
        if (messageBuilder != null) {
            return (MessageBuilder) Utils.loadMessageBuilder(messageBuilder);
        } else {
            return new DefaultKafkaMessageBuilder();
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
        return configuration.getProperties();
    }

    @Override
    public DestinationSelector getDestinationSelector() {
        return new DestinationSelector() {
            @Override
            public String select(Tuple tuple) {
                Object o = tuple.getValueByField("sensorID");
                return o.toString();
            }
        };
    }

    @Override
    public DestinationChanger getDestinationChanger() {
        return new ZKDestinationChanger(topologyName, sensor, channel, zkConnectionString, true);
    }
}

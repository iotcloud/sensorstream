package cgl.sensorstream.core.rabbitmq;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import com.ss.rabbitmq.MessageBuilder;
import com.ss.rabbitmq.RabbitMQConfigurator;
import com.ss.rabbitmq.RabbitMQDestination;
import com.ss.rabbitmq.RabbitMQDestinationSelector;

import java.util.ArrayList;
import java.util.List;

public class RabbitMQSpoutConfigurator implements RabbitMQConfigurator {
    private String url;

    private List<RabbitMQDestination> destinations = new ArrayList<RabbitMQDestination>();

    private MessageBuilder messageBuilder;

    private List<String> outFields = new ArrayList<String>();

    public RabbitMQSpoutConfigurator(String url, List<RabbitMQDestination> destinations, MessageBuilder messageBuilder, List<String> outFields) {
        this.url = url;
        this.destinations = destinations;
        this.messageBuilder = messageBuilder;
        this.outFields = outFields;
    }

    @Override
    public String getURL() {
        return this.url;
    }

    @Override
    public boolean isAutoAcking() {
        return true;
    }

    @Override
    public int getPrefetchCount() {
        return 0;
    }

    @Override
    public boolean isReQueueOnFail() {
        return false;
    }

    @Override
    public String getConsumerTag() {
        return null;
    }

    @Override
    public List<RabbitMQDestination> getQueueName() {
        return destinations;
    }

    @Override
    public MessageBuilder getMessageBuilder() {
        return this.messageBuilder;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(this.outFields));
    }

    @Override
    public int queueSize() {
        return 64;
    }

    @Override
    public RabbitMQDestinationSelector getDestinationSelector() {
        return null;
    }
}

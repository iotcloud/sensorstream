package cgl.sensorstream.core.kafka;

import backtype.storm.topology.OutputFieldsDeclarer;
import com.ss.commons.DestinationChanger;
import com.ss.commons.MessageBuilder;
import com.ss.commons.SpoutConfigurator;

import java.util.Map;

public class KafkaSpoutConfigurator implements SpoutConfigurator{
    @Override
    public MessageBuilder getMessageBuilder() {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public int queueSize() {
        return 0;
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public DestinationChanger getDestinationChanger() {
        return null;
    }
}

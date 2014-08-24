package cgl.sensorstream.core.kafka;

import backtype.storm.topology.OutputFieldsDeclarer;
import com.ss.commons.BoltConfigurator;
import com.ss.commons.DestinationChanger;
import com.ss.commons.DestinationSelector;
import com.ss.commons.MessageBuilder;

import java.util.Map;

public class KafkaBoltConfigurator implements BoltConfigurator {
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
    public DestinationSelector getDestinationSelector() {
        return null;
    }

    @Override
    public DestinationChanger getDestinationChanger() {
        return null;
    }
}

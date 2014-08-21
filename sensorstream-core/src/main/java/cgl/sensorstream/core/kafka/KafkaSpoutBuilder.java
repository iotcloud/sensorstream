package cgl.sensorstream.core.kafka;

import backtype.storm.topology.IRichSpout;
import cgl.sensorstream.core.SpoutBuilder;

import java.util.List;
import java.util.Map;

public class KafkaSpoutBuilder implements SpoutBuilder {
    @Override
    public IRichSpout build(String sensor, String channel, List<String> fields, String convertor, Map<String, Object> properties, String zkConnection) {
        return null;
    }
}

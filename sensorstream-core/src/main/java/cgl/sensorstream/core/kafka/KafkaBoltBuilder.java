package cgl.sensorstream.core.kafka;

import backtype.storm.topology.IRichBolt;
import cgl.sensorstream.core.BoltBuilder;

import java.util.List;
import java.util.Map;

public class KafkaBoltBuilder implements BoltBuilder {
    @Override
    public IRichBolt build(String sensor, String channel,
                           List<String> fields, String convertor,
                           Map<String, Object> properties, String zkConnection) {


        return null;
    }
}

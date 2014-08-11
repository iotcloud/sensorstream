package cgl.sensorstream.core;

import backtype.storm.topology.IRichBolt;

import java.util.List;
import java.util.Map;

public interface BoltBuilder {
    public IRichBolt build(String sensor, String channel, List<String> fields, String convertor, Map<String, Object> properties, String zkConnection);
}

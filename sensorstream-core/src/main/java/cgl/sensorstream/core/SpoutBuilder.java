package cgl.sensorstream.core;

import backtype.storm.spout.ISpout;

import java.util.List;
import java.util.Map;

public interface SpoutBuilder {
    public ISpout build(String sensor, String channel, List<String> fields, String convertor, Map<String, Object> properties, String zkConnection);
}

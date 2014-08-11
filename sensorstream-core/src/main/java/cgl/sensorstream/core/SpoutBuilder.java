package cgl.sensorstream.core;

import backtype.storm.topology.IRichSpout;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public interface SpoutBuilder extends Serializable {
    public IRichSpout build(String sensor, String channel, List<String> fields, String convertor, Map<String, Object> properties, String zkConnection);
}

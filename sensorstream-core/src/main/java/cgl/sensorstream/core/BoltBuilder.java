package cgl.sensorstream.core;

import backtype.storm.task.IBolt;

import java.util.List;
import java.util.Map;

public interface BoltBuilder {
    public IBolt build(String sensor, String channel, List<String> fields, String convertor, Map<String, Object> properties, String zkConnection);
}

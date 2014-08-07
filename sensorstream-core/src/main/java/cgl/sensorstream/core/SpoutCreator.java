package cgl.sensorstream.core;

import backtype.storm.spout.ISpout;

import java.util.List;
import java.util.Map;

public interface SpoutCreator {
    public ISpout create(String url, List<String> fields, Object convertor, Map<String, String> properties);
}

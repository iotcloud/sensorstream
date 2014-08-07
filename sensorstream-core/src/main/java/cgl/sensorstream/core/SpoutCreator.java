package cgl.sensorstream.core;

import backtype.storm.spout.ISpout;

import java.util.List;

public interface SpoutCreator {
    public ISpout create(String url, List<String> fields, );
}

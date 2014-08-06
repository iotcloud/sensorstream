package cgl.sensorstream.core;

import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;

import java.util.HashMap;
import java.util.Map;

public class StreamComponents {
    private Map<String, ISpout> spouts = new HashMap<String, ISpout>();

    private Map<String, IBolt> bolts = new HashMap<String, IBolt>();

    public void addSpout(String name, ISpout spout) {
        spouts.put(name, spout);
    }

    public void addBolt(String name, IBolt bolt) {
        bolts.put(name, bolt);
    }

    public Map<String, ISpout> getSpouts() {
        return spouts;
    }

    public Map<String, IBolt> getBolts() {
        return bolts;
    }
}

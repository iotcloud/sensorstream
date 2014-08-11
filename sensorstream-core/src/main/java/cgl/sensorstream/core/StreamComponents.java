package cgl.sensorstream.core;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;

import java.util.HashMap;
import java.util.Map;

public class StreamComponents {
    private Map<String, IRichSpout> spouts = new HashMap<String, IRichSpout>();

    private Map<String, IRichBolt> bolts = new HashMap<String, IRichBolt>();

    public void addSpout(String name, IRichSpout spout) {
        spouts.put(name, spout);
    }

    public void addBolt(String name, IRichBolt bolt) {
        bolts.put(name, bolt);
    }

    public Map<String, IRichSpout> getSpouts() {
        return spouts;
    }

    public Map<String, IRichBolt> getBolts() {
        return bolts;
    }
}

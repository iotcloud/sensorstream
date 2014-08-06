package cgl.sensorstream.core;

import cgl.sensorstream.core.config.Configuration;

import java.util.Map;

public class StreamTopologyBuilder {
    public StreamComponents buildComponents() {
        Map conf = Utils.readStreamConfig();

        String zkServers = Configuration.getZkConnection(conf);

        return null;
    }
}

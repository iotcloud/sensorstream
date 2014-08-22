package cgl.sensorstream.core;

import com.ss.commons.DestinationChangeListener;
import com.ss.commons.DestinationChanger;

public class ZKDestinationChanger implements DestinationChanger {
    private SensorListener listener;

    private DestinationChangeListener dstListener;

    private String sensor;

    private String channel;

    private String zkConnectionString;

    private String topologyName;

    public ZKDestinationChanger(String topologyName, String sensor, String channel, String zkConnectionString) {
        this.sensor = sensor;
        this.channel = channel;
        this.zkConnectionString = zkConnectionString;
        this.topologyName = topologyName;
    }

    @Override
    public void start() {
        listener = new SensorListener(topologyName, sensor, channel, zkConnectionString, dstListener);
        listener.start();
    }

    @Override
    public void stop() {
        listener.close();
    }

    @Override
    public void registerListener(DestinationChangeListener destinationChangeListener) {
        dstListener = destinationChangeListener;
    }
}

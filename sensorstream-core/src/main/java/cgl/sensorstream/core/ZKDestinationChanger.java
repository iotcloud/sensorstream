package cgl.sensorstream.core;

import com.ss.commons.DestinationChangeListener;
import com.ss.commons.DestinationChanger;

public class ZKDestinationChanger implements DestinationChanger {
    private SensorListener listener;

    private DestinationChangeListener dstListener;

    private String sensor;

    private String channel;

    private String zkConnectionString;

    public ZKDestinationChanger(String sensor, String channel, String zkConnectionString) {
        this.sensor = sensor;
        this.channel = channel;
        this.zkConnectionString = zkConnectionString;
    }

    @Override
    public void start() {
        listener = new SensorListener(sensor, channel, zkConnectionString, dstListener);
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

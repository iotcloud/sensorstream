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

    private int totalTasks;

    private int taskIndex;

    public ZKDestinationChanger(String topologyName, String sensor, String channel, String zkConnectionString) {
        this.sensor = sensor;
        this.channel = channel;
        this.zkConnectionString = zkConnectionString;
        this.topologyName = topologyName;
    }

    @Override
    public void start() {
        listener = new SensorListener(topologyName, sensor, channel, zkConnectionString, dstListener, taskIndex, totalTasks);
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

    @Override
    public void setTask(int taskIndex, int totalTasks) {
        this.taskIndex = taskIndex;
        this.totalTasks = totalTasks;
    }

    @Override
    public int getTaskIndex() {
        return taskIndex;
    }

    @Override
    public int getTotalTasks() {
        return totalTasks;
    }
}

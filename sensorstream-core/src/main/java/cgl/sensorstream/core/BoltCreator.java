package cgl.sensorstream.core;

import backtype.storm.task.IBolt;

public interface BoltCreator {
    public IBolt create();
}

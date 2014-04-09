package cgl.sensorstream.storm;

import javax.jms.Destination;

public class Notification {
    public Notification(String path, Destination destination, Type add) {

    }

    public enum Type {
        ADD,
        REMOVE
    }
}

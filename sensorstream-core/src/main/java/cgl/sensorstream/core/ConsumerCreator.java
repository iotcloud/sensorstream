package cgl.sensorstream.core;

import cgl.iotcloud.core.api.thrift.TChannel;

public interface ConsumerCreator {
    public Object createConsumer(TChannel channel);
}

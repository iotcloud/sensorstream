package cgl.sensorstream.sensors.kafka;

import cgl.iotcloud.core.AbstractSensor;
import cgl.iotcloud.core.Configurator;
import cgl.iotcloud.core.SensorContext;

import java.util.Map;

public class KafkaPerfSensor extends AbstractSensor {
    @Override
    public Configurator getConfigurator(Map map) {
        return null;
    }

    @Override
    public void open(SensorContext context) {

    }

    @Override
    public void close() {

    }
}

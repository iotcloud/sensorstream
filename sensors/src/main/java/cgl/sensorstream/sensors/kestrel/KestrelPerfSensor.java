package cgl.sensorstream.sensors.kestrel;

import cgl.iotcloud.core.AbstractSensor;
import cgl.iotcloud.core.Configurator;
import cgl.iotcloud.core.SensorContext;

import java.util.Map;

public class KestrelPerfSensor extends AbstractSensor {
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

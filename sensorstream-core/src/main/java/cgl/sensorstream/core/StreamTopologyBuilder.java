package cgl.sensorstream.core;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import cgl.sensorstream.core.config.Configuration;
import cgl.sensorstream.core.rabbitmq.RabbitMQBoltBuilder;
import cgl.sensorstream.core.rabbitmq.RabbitMQSpoutBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamTopologyBuilder {
    private static Logger LOG = LoggerFactory.getLogger(StreamTopologyBuilder.class);

    public static final String SPOUTS = "spouts";
    public static final String BOLTS = "bolts";
    public static final String CHANNEL = "channel";
    public static final String FIELDS = "fields";
    public static final String BUILDER = "builder";
    public static final String BROKER = "broker";
    public static final String SENSOR = "sensor";
    public static final String PROPERTIES = "properties";

    private Map<String, SpoutBuilder> spoutBuilders = new HashMap<String, SpoutBuilder>();

    private Map<String, BoltBuilder> boltBuilders = new HashMap<String, BoltBuilder>();

    private String zkServers;

    public StreamTopologyBuilder() {
        spoutBuilders.put("rabbitmq", new RabbitMQSpoutBuilder());
        boltBuilders.put("rabbitmq", new RabbitMQBoltBuilder());
    }

    public StreamComponents buildComponents() {
        StreamComponents components = new StreamComponents();

        Map conf = Utils.readStreamConfig();

        zkServers = Configuration.getZkConnection(conf);

        Map spoutsMap = (Map) conf.get(SPOUTS);
        if (spoutsMap != null) {
            for (Object e : spoutsMap.entrySet()) {
                if (e instanceof Map.Entry) {
                    Object name = ((Map.Entry) e).getKey();
                    Object spoutConf = ((Map.Entry) e).getValue();

                    if (!(name instanceof String)) {
                        String msg = "The spout name should be an string";
                        LOG.error(msg);
                        throw new RuntimeException(msg);
                    }

                    if (!(spoutConf instanceof Map)) {
                        String msg = "The spout configurations should be in a map";
                        LOG.error(msg);
                        throw new RuntimeException(msg);
                    }

                    IRichSpout spout = buildSpout(conf, (Map) spoutConf);
                    components.addSpout((String) name, spout);
                } else {
                    String s = "The spout configuration should be a map";
                    LOG.error(s);
                    throw new RuntimeException(s);
                }
            }
        }

        Map boltsMap = (Map) conf.get(BOLTS);
        if (boltsMap != null) {
            for (Object e : boltsMap.entrySet()) {
                if (e instanceof Map.Entry) {
                    Object name = ((Map.Entry) e).getKey();
                    Object spoutConf = ((Map.Entry) e).getValue();

                    if (!(name instanceof String)) {
                        String msg = "The spout name should be an string";
                        LOG.error(msg);
                        throw new RuntimeException(msg);
                    }

                    if (!(spoutConf instanceof Map)) {
                        String msg = "The spout configurations should be in a map";
                        LOG.error(msg);
                        throw new RuntimeException(msg);
                    }

                    IRichBolt bolt = buildBolt(conf, (Map) spoutConf);
                    components.addBolt((String) name, bolt);
                } else {
                    String s = "The spout configuration should be a map";
                    LOG.error(s);
                    throw new RuntimeException(s);
                }
            }
        }

        return components;
    }

    private IRichSpout buildSpout(Map conf, Map spoutConf) {
        Object channelConf = spoutConf.get(CHANNEL);
        if (!(channelConf instanceof String)) {
            String msg = "The channels should be a string";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        Object sensorConf = spoutConf.get(SENSOR);
        if (!(sensorConf instanceof String)) {
            String msg = "The sensor should be a string";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        Object filedsConf = spoutConf.get(FIELDS);
        if (!(filedsConf instanceof List)) {
            String msg = "The fields should be a string list";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        List<String> fields = new ArrayList<String>();
        for (Object o : (List)filedsConf) {
            fields.add(o.toString());
        }

        Object conversion = spoutConf.get(BUILDER);
        String messageBuilder = null;
        if (conversion != null) {
            if (!(conversion instanceof String)) {
                String msg = "The messageBuilder should specify a message builder implementation";
                LOG.error(msg);
                throw new RuntimeException(msg);
            }
            messageBuilder = conversion.toString();
        }

        Object broker = spoutConf.get(BROKER);
        if (!(broker instanceof String)) {
            String msg = "The conversion should specify a message converter";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        Object properties = spoutConf.get(PROPERTIES);
        if (properties != null && !(properties instanceof Map)) {
            String msg = "The properties should be a map";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        SpoutBuilder builder = spoutBuilders.get(broker.toString());
        if (builder == null) {
            String msg = "Cannot build a spout of type: " + broker;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        return builder.build(sensorConf.toString(), channelConf.toString(), fields, messageBuilder, (Map<String, Object>) properties, zkServers);
    }

    private IRichBolt buildBolt(Map conf, Map spoutConf) {
        Object channelConf = spoutConf.get(CHANNEL);
        if (!(channelConf instanceof String)) {
            String msg = "The channels should be a string";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        Object sensorConf = spoutConf.get(SENSOR);
        if (!(sensorConf instanceof String)) {
            String msg = "The sensor should be a string";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        Object filedsConf = spoutConf.get(FIELDS);
        if (!(filedsConf instanceof List)) {
            String msg = "The fields should be a string list";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        List<String> fields = new ArrayList<String>();
        for (Object o : (List)filedsConf) {
            fields.add(o.toString());
        }

        Object conversion = spoutConf.get(BUILDER);
        String messageBuilder = null;
        if (conversion != null) {
            if (!(conversion instanceof String)) {
                String msg = "The messageBuilder should specify a message builder implementation";
                LOG.error(msg);
                throw new RuntimeException(msg);
            }
            messageBuilder = conversion.toString();
        }

        Object broker = spoutConf.get(BROKER);
        if (!(broker instanceof String)) {
            String msg = "The conversion should specify a message converter";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        Object properties = spoutConf.get(PROPERTIES);
        if (properties != null && !(properties instanceof Map)) {
            String msg = "The properties should be a map";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        BoltBuilder builder = boltBuilders.get(broker.toString());
        if (builder == null) {
            String msg = "Cannot build a spout of type: " + broker;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        return builder.build(sensorConf.toString(), channelConf.toString(), fields, messageBuilder, (Map<String, Object>) properties, zkServers);
    }
}



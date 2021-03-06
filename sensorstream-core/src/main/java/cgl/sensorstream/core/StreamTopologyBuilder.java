package cgl.sensorstream.core;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import cgl.sensorstream.core.config.Configuration;
import cgl.sensorstream.core.kafka.KafkaBoltBuilder;
import cgl.sensorstream.core.kafka.KafkaSpoutBuilder;
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
    public static final String STREAM = "stream";
    public static final String SENSOR = "sensor";
    public static final String PROPERTIES = "properties";
    public static final String CONF = "conf";

    private Map<String, SpoutBuilder> spoutBuilders = new HashMap<String, SpoutBuilder>();

    private Map<String, BoltBuilder> boltBuilders = new HashMap<String, BoltBuilder>();

    private String topologyFile = null;

    private TopologyConfiguration topologyConfiguration;

    public StreamTopologyBuilder(String topologyFile) {
        this.topologyFile = topologyFile;
        spoutBuilders.put("rabbitmq", new RabbitMQSpoutBuilder());
        boltBuilders.put("rabbitmq", new RabbitMQBoltBuilder());

        spoutBuilders.put("kafka", new KafkaSpoutBuilder());
        boltBuilders.put("kafka", new KafkaBoltBuilder());
    }

    public StreamTopologyBuilder() {
        this(null);
    }

    public StreamComponents buildComponents() {
        StreamComponents components = new StreamComponents();

        Map conf;
        if (topologyFile == null) {
            conf = Utils.readStreamConfig();
        } else {
            conf = Utils.readStreamConfig(topologyFile);
        }

        String zkServers = Configuration.getZkConnection(conf);
        String zkRoot = Configuration.getZkRoot(conf);
        String topologyName = Configuration.getTopologyName(conf);
        if (topologyName == null) {
            topologyName = "default";
        }
        topologyConfiguration = new TopologyConfiguration(topologyName, zkServers, zkRoot);

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

                    IRichSpout spout = buildSpout(topologyConfiguration, (Map) spoutConf);
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

                    IRichBolt bolt = buildBolt(topologyConfiguration, (Map) spoutConf);
                    components.addBolt((String) name, bolt);
                } else {
                    String s = "The spout configuration should be a map";
                    LOG.error(s);
                    throw new RuntimeException(s);
                }
            }
        }

        Map configuration = (Map) conf.get(CONF);
        components.setConf(configuration);

        return components;
    }

    private IRichSpout buildSpout(TopologyConfiguration topologyConfiguration, Map spoutConf) {
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

        String streamVal = null;
        Object stream = spoutConf.get(STREAM);
        if (stream != null) {
            if (!(stream instanceof String)) {
                String msg = "The stream should be a string";
                LOG.error(msg);
                throw new RuntimeException(msg);
            }
            streamVal = (String) stream;
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

        ComponentConfiguration configuration = new ComponentConfiguration(topologyConfiguration, sensorConf.toString(),
                channelConf.toString(), fields, messageBuilder);
        configuration.setStream(streamVal);
        if (properties != null) {
            for (Object o : ((Map) properties).keySet()) {
                configuration.addProperty(o.toString(), ((Map) properties).get(o).toString());
            }
        }
        return builder.build(configuration);
    }

    private IRichBolt buildBolt(TopologyConfiguration topologyConfiguration, Map boltConf) {
        Object channelConf = boltConf.get(CHANNEL);
        if (!(channelConf instanceof String)) {
            String msg = "The channels should be a string";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        Object sensorConf = boltConf.get(SENSOR);
        if (!(sensorConf instanceof String)) {
            String msg = "The sensor should be a string";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        Object filedsConf = boltConf.get(FIELDS);
        if (!(filedsConf instanceof List)) {
            String msg = "The fields should be a string list";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        List<String> fields = new ArrayList<String>();
        for (Object o : (List)filedsConf) {
            fields.add(o.toString());
        }

        Object conversion = boltConf.get(BUILDER);
        String messageBuilder = null;
        if (conversion != null) {
            if (!(conversion instanceof String)) {
                String msg = "The messageBuilder should specify a message builder implementation";
                LOG.error(msg);
                throw new RuntimeException(msg);
            }
            messageBuilder = conversion.toString();
        }

        Object broker = boltConf.get(BROKER);
        if (!(broker instanceof String)) {
            String msg = "The conversion should specify a message converter";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        Object properties = boltConf.get(PROPERTIES);
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
        ComponentConfiguration configuration = new ComponentConfiguration(topologyConfiguration, sensorConf.toString(),
                channelConf.toString(), fields, messageBuilder);
        if (properties != null) {
            for (Object o : ((Map) properties).keySet()) {
                configuration.addProperty(o.toString(), ((Map) properties).get(o).toString());
            }
        }

        return builder.build(configuration);
    }
}



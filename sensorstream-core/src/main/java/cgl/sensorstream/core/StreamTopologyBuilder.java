package cgl.sensorstream.core;

import backtype.storm.spout.ISpout;
import cgl.iotcloud.core.api.thrift.TChannel;
import cgl.iotcloud.core.api.thrift.TDirection;
import cgl.iotcloud.core.utils.SerializationUtils;
import cgl.sensorstream.core.config.Configuration;
import cgl.sensorstream.core.rabbitmq.RabbitMQSpoutCreator;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StreamTopologyBuilder {
    private static Logger LOG = LoggerFactory.getLogger(StreamTopologyBuilder.class);

    public static final String SPOUTS = "spouts";
    public static final String CHANNELS = "channels";
    public static final String FIELDS = "fields";
    public static final String CONVERSION = "conversion";
    public static final String PARALLELISM = "parallelism";

    private Map<String, SpoutCreator> spoutCreatorMap = new HashMap<String, SpoutCreator>();

    private Map<String, SpoutCreator> boltCreatorMap = new HashMap<String, SpoutCreator>();

    private CuratorFramework curatorFramework;

    public StreamTopologyBuilder() {
        spoutCreatorMap.put("rabbitmq", new RabbitMQSpoutCreator());
    }

    public StreamComponents buildComponents() {
        StreamComponents components = new StreamComponents();

        Map conf = Utils.readStreamConfig();

        String zkServers = Configuration.getZkConnection(conf);

        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorFramework = CuratorFrameworkFactory.newClient(zkServers, retryPolicy);

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

                    ISpout spout = buildSpout(conf, (Map) spoutConf);
                    components.addSpout((String) name, spout);
                } else {
                    String s = "The spout configuration should be a map";
                    LOG.error(s);
                    throw new RuntimeException(s);
                }
            }
        }


        return null;
    }

    private ISpout buildSpout(Map conf, Map spoutConf) {
        Object channelsConf = spoutConf.get(CHANNELS);
        List<String> channels = new ArrayList<String>();
        List<String> fields = new ArrayList<String>();
        if (!(channelsConf instanceof List)) {
            String msg = "The channels should be a list";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        for (Object o : (List)channelsConf) {
            channels.add(o.toString());
        }

        Object filedsConf = spoutConf.get(FIELDS);
        if (!(filedsConf instanceof List)) {
            String msg = "The fields should be a list";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        for (Object o : (List)filedsConf) {
            fields.add(o.toString());
        }

        Object conversion = spoutConf.get(CONVERSION);
        if (!(conversion instanceof String)) {
            String msg = "The conversion should specify a message converter";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        Object parallel = spoutConf.get(PARALLELISM);
        if (!(parallel instanceof Integer)) {
            String msg = "The parallelism should be a integer";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        // get the channels from zoo keeper
        for (String channel : channels) {
            TChannel tChannel = getChannel(channel, conf);

            if (tChannel.getDirection() == TDirection.OUT) {
                String transport = tChannel.getTransport();
                SpoutCreator creator = spoutCreatorMap.get(transport);

                Object messageBuilder = loadMessageBuilder(conversion.toString());
                ISpout spout = creator.create(tChannel.getBrokerUrl(), fields, messageBuilder, tChannel.getProperties());
            }
        }

        return ;
    }

    private Object loadMessageBuilder(String path) {
        try {
            Class<?> c = Class.forName(path);
            Object t = c.newInstance();
            return t;
        } catch (InstantiationException e) {
            String msg = "Failed to initialize the class: " + path;
            LOG.error(msg);
            throw new RuntimeException(msg, e);
        } catch (IllegalAccessException e) {
            String msg = "Failed to access the class: " + path;
            LOG.error(msg);
            throw new RuntimeException(msg, e);
        } catch (ClassNotFoundException e) {
            String msg = "The class: " + path + " cannot be found";
            LOG.error(msg);
            throw new RuntimeException(msg, e);
        }
    }

    private String getChannelPath(String name, Map conf) {
        return Configuration.getZkRoot(conf) + "/" + Configuration.getChannelsPath(conf) + "/" + name;
    }

    private TChannel getChannel(String name, Map conf) {
        try {
            byte channelData[] = curatorFramework.getData().forPath(getChannelPath(name, conf));

            TChannel channel = new TChannel();
            SerializationUtils.createThriftFromBytes(channelData, channel);
            return channel;
        } catch (Exception e) {
            String msg = "Failed to get the data for channel: " + name;
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
    }
}



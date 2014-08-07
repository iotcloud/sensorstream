package cgl.sensorstream.core;

import backtype.storm.spout.ISpout;
import cgl.sensorstream.core.config.Configuration;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class StreamTopologyBuilder {
    private static Logger LOG = LoggerFactory.getLogger(StreamTopologyBuilder.class);

    public static final String SPOUTS = "spouts";
    public static final String CHANNELS = "channels";
    public static final String FIELDS = "fields";
    public static final String CONVERSION = "conversion";
    public static final String PARALLELISM = "parallelism";

    public StreamComponents buildComponents() {
        StreamComponents components = new StreamComponents();

        Map conf = Utils.readStreamConfig();

        String zkServers = Configuration.getZkConnection(conf);

        CuratorFramework curatorFramework = null;
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

                    ISpout spout = buildSpout((Map) spoutConf);
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

    private ISpout buildSpout(Map spoutConf) {
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

        Object parallel = spoutConf.get(CONVERSION);
        if (!(parallel instanceof Integer)) {
            String msg = "The parallelism should be a integer";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }

        // get the channels from zoo keeper


        return  null;
    }
}



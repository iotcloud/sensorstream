package cgl.sensorstream.core.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class Configuration {
    private static Logger LOG = LoggerFactory.getLogger(Configuration.class);

    // constants for configurations
    // the iot server url
    public static final String SS_IOT_SERVER = "localhost";
       
    // the zookeeper port
    public static final String SS_ZOOKEEPER_PORT = "storm.zookeeper.port";
    
    // the zookeeper host
    public static final String SS_ZOOKEEPER_SERVERS = "storm.zookeeper.servers";
    

    public static final String SS_ZOOKEEPER_SESSION_TIMEOUT = "ss.zookeeper.session.timeout";
    
    public static final String SS_ZOOKEEPER_CONNECTION_TIMEOUT = "ss.zookeeper.connection.timeout";
    
    public static final String SS_ZOOKEEPER_RETRY_TIMES = "ss.zookeeper.retry.times";

    public static final String SS_ZOOKEEPER_RETRY_INTERVAL = "ss.zookeeper.retry.interval";

    public static final String SS_ZOOKEEPER_RETRY_INTERVALCEILING_MILLIS = "ss.zookeeper.retry.intervalceiling.millis";

    public static final String SS_ZOOKEEPER_UPDATES_SIZE = "ss.zookeeper.updates.size";

    public static final String ZK_SERVERS = "zk.servers";

    public static String getZkConnection(Map conf) {
        Object o = conf.get(ZK_SERVERS);
        StringBuilder buffer = new StringBuilder();
        if (o instanceof List) {
            for (Object s : (List)o) {
                buffer.append(s.toString());
            }
        }
        return buffer.toString();
    }

    public static final String ZK_ROOT = "zk.root";

    public static String getZkRoot(Map conf) {
        Object o = conf.get(ZK_ROOT);
        return o.toString();
    }

    public static final String ZK_CHANNELS_PATH = "zk.channels.path";

    public static String getChannelsPath(Map conf) {
        Object o = conf.get(ZK_CHANNELS_PATH);
        return o.toString();
    }

    public static final String TOPOLOGY_NAME = "topology.name";

    public static String getTopologyName(Map conf) {
        Object o = conf.get(TOPOLOGY_NAME);
        return o.toString();
    }
}

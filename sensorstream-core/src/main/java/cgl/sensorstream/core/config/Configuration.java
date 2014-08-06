package cgl.sensorstream.core.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.management.resources.agent_pt_BR;

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
    
    public static final String SS_ZOOKEEPER_ROOT = "ss.zookeeper.root";
    
    public static final String SS_ZOOKEEPER_SESSION_TIMEOUT = "ss.zookeeper.session.timeout";
    
    public static final String SS_ZOOKEEPER_CONNECTION_TIMEOUT = "ss.zookeeper.connection.timeout";
    
    public static final String SS_ZOOKEEPER_RETRY_TIMES = "ss.zookeeper.retry.times";

    public static final String SS_ZOOKEEPER_RETRY_INTERVAL = "ss.zookeeper.retry.interval";

    public static final String SS_ZOOKEEPER_RETRY_INTERVALCEILING_MILLIS = "ss.zookeeper.retry.intervalceiling.millis";

    public static final String SS_ZOOKEEPER_UPDATES_SIZE = "ss.zookeeper.updates.size";

    public static String getZkRoot(Map conf) {
        return (String) conf.get(SS_ZOOKEEPER_ROOT);
    }

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
}

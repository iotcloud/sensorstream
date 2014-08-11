package cgl.sensorstream.core;

import cgl.iotcloud.core.api.thrift.TChannel;
import cgl.sensorstream.core.config.Configuration;
import com.ss.commons.DestinationConfiguration;
import org.ho.yaml.Yaml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.*;

public class Utils {
    private static Logger LOG = LoggerFactory.getLogger(Utils.class);

    public static Map findAndReadConfigFile(String name, boolean mustExist) {
        try {
            HashSet<URL> resources = new HashSet<URL>(findResources(name));
            if (resources.isEmpty()) {
                if (mustExist) throw new RuntimeException("Could not find config file on classpath " + name);
                else return new HashMap();
            }
            if (resources.size() > 1) {
                throw new RuntimeException("Found multiple " + name + " resources."
                        + resources);
            }
            URL resource = resources.iterator().next();
            Map ret = (Map) Yaml.load(new InputStreamReader(resource.openStream()));

            if (ret == null) ret = new HashMap();
            return new HashMap(ret);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL> ret = new ArrayList<URL>();
            while (resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Map findAndReadConfigFile(String name) {
        return findAndReadConfigFile(name, true);
    }

    public static Map readStreamConfig() {
        String confFile = System.getProperty("stream.conf.file");
        Map conf;
        if (confFile == null || confFile.equals("")) {
            conf = findAndReadConfigFile("topology.yaml", false);
        } else {
            conf = findAndReadConfigFile(confFile);
        }
        return conf;
    }

    public static String getZkConnectionString(Map conf) {
        int port = (Integer) conf.get(Configuration.SS_ZOOKEEPER_PORT);
        List servers = (List) conf.get(Configuration.SS_ZOOKEEPER_SERVERS);

        return servers.get(0) + ":" + port;
    }

    public static Object loadMessageBuilder(String path) {
        try {
            Class<?> c = Class.forName(path);
            return c.newInstance();
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

    public static DestinationConfiguration convertChannelToDestination(TChannel channel) {
        DestinationConfiguration destinationConfiguration = new DestinationConfiguration(channel.getName(), channel.getBrokerUrl(), channel.getSite(), channel.getSensor());
        if (channel.getProperties() != null) {
            for (Map.Entry<String, String> entry : channel.getProperties().entrySet()) {
                destinationConfiguration.addProperty(entry.getKey(), entry.getValue());
            }
        }
        return destinationConfiguration;
    }

}

package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import org.apache.commons.cli.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractPerfTopology {
    public static TopologyConfiguration parseArgs(String []args, Map<String, String> properties) {
        Options options = new Options();
        options.addOption("n", true, "No of queues");
        options.addOption("i", true, "IP");
        options.addOption("qr", true, "Receive Queue name");
        options.addOption("qs", true, "Send Queue name");
        options.addOption("name", false, "Topology name");
        options.addOption("nw", false, "No of workers");
        options.addOption("local", false, "Weather local storm is used");

        if (properties != null) {
            for (Map.Entry<String, String> e : properties.entrySet()) {
                options.addOption(e.getKey(), true, e.getValue());
            }
        }

        CommandLineParser commandLineParser = new BasicParser();
        try {
            CommandLine cmd = commandLineParser.parse(options, args);

            String ips = cmd.getOptionValue("i");
            String noQueues =  cmd.getOptionValue("n");
            String rQueue =  cmd.getOptionValue("qr");
            String sQueue = cmd.getOptionValue("qs");
            String tpName = cmd.getOptionValue("name");
            String noWorkers = cmd.getOptionValue("nw");



            boolean local = cmd.hasOption("local");

            String[] results = ips.split(",");
            List<String> ipList = new ArrayList<String>();
            for (String ip : results) {
                ipList.add(ip.trim());
            }

            TopologyConfiguration tpConfiguration = new TopologyConfiguration(ipList, Integer.parseInt(noQueues), rQueue, sQueue);
            if (tpName != null) {
                tpConfiguration.setTopologyName(tpName);
            }
            if (noWorkers != null) {
                tpConfiguration.setNoWorkers(Integer.parseInt(noWorkers));
            }

            tpConfiguration.setLocal(local);

            if (properties != null) {
                for (Map.Entry<String, String> e : properties.entrySet()) {
                    tpConfiguration.addProperty(e.getKey(), cmd.getOptionValue(e.getKey()));
                }
            }

            return tpConfiguration;
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("sensor", options );
        }

        return null;
    }

    public static TopologyConfiguration parseArgs(String []args) {
        return parseArgs(args, null);
    }

    public static boolean isLocal(String []args) {
        for (String s : args) {
            if (s.equals("local")) {
                return true;
            }
        }
        return false;
    }

    public static void submit(String []args, String topologyName,
                              TopologyBuilder builder, TopologyConfiguration configuration) throws Exception {
        Config conf = new Config();
        if (!configuration.isLocal()) {
            conf.setNumWorkers(configuration.getNoWorkers());
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Thread.sleep(6000000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
    }

    public static void submit(String []args, String topologyName,
                              TopologyBuilder builder, TopologyConfiguration configuration, Config conf) throws Exception {
        if (!configuration.isLocal()) {
            conf.setNumWorkers(configuration.getNoWorkers());
            StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topologyName, conf, builder.createTopology());
            Thread.sleep(6000000);
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
    }
}

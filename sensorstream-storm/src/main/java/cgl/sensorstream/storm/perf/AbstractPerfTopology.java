package cgl.sensorstream.storm.perf;

import org.apache.commons.cli.*;

public abstract class AbstractPerfTopology {
    public static TopologyConfiguration parseArgs(String []args) {
        Options options = new Options();
        options.addOption("n", true, "No of queues");
        options.addOption("i", true, "IP");
        options.addOption("qr", true, "Receive Queue name");
        options.addOption("qs", true, "Send Queue name");
        options.addOption("name", false, "Topology name");
        options.addOption("nw", false, "No of workers");

        CommandLineParser commandLineParser = new BasicParser();
        try {
            CommandLine cmd = commandLineParser.parse(options, args);

            String ip = cmd.getOptionValue("i");
            String noQueues =  cmd.getOptionValue("n");
            String rQueue =  cmd.getOptionValue("qr");
            String sQueue = cmd.getOptionValue("qs");
            String tpName = cmd.getOptionValue("name");
            String noWorkers = cmd.getOptionValue("nw");

            TopologyConfiguration tpConfiguration = new TopologyConfiguration(ip, Integer.parseInt(noQueues), rQueue, sQueue);
            if (tpName != null) {
                tpConfiguration.setTopologyName(tpName);
            }
            if (noWorkers != null) {
                tpConfiguration.setNoWorkers(Integer.parseInt(noWorkers));
            }
            return tpConfiguration;
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("sensor", options );
        }

        return null;
    }
}

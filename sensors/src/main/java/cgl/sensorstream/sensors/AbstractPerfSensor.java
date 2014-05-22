package cgl.sensorstream.sensors;

import cgl.iotcloud.core.AbstractSensor;
import cgl.iotcloud.core.SensorContext;
import cgl.iotcloud.core.Utils;
import cgl.iotcloud.core.client.SensorClient;
import cgl.iotcloud.core.sensorsite.SensorDeployDescriptor;
import org.apache.commons.cli.*;
import org.apache.thrift.transport.TTransportException;

import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class AbstractPerfSensor extends AbstractSensor {
    public static final String SEND_QUEUE_NAME_PROP = "send_queue";
    public static final String RECEIVE_QUEUE_PROP = "recv_queue";

    public static final String SEND_INTERVAL = "send_interval";
    public static final String FILE_NAME = "file_name";


    public static void deploy(String args[], List<String> sites, String className) throws TTransportException {
        // read the configuration file
        Map conf = Utils.readConfig();
        SensorClient client;
        client = new SensorClient(conf);

        SensorConfiguration configuration = parseArgs(args);
        for (int i = 0; i < configuration.getNoSensors() / sites.size(); i++) {
            SensorDeployDescriptor deployDescriptor = new SensorDeployDescriptor("sensors-1.0-SNAPSHOT-jar-with-dependencies.jar", className);
            deployDescriptor.addDeploySites(sites);
            addConfigurationsToDescriptor(configuration, deployDescriptor, i + 1);
            client.deploySensor(deployDescriptor);
        }
    }

    public static SensorConfiguration parseArgs(String []args) {
        Options options = new Options();
        options.addOption("t", true, "Time interval");
        options.addOption("f", true, "File name");
        options.addOption("n", true, "Number of sensors");
        options.addOption("qr", true, "Receive Queue name");
        options.addOption("qs", true, "Send Queue name");

        CommandLineParser commandLineParser = new BasicParser();
        try {
            CommandLine cmd = commandLineParser.parse(options, args);

            String timeString = cmd.getOptionValue("t", "100");
            String fileName = cmd.getOptionValue("f");
            int noSensors = Integer.parseInt(cmd.getOptionValue("n"));
            String recvQueueName = cmd.getOptionValue("qr");
            String sendQueueName = cmd.getOptionValue("qs");

            return new SensorConfiguration(noSensors, sendQueueName, recvQueueName, Integer.parseInt(timeString), fileName);
        } catch (ParseException e) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("sensor", options );
        }
        return null;
    }

    public static void addConfigurationsToDescriptor(SensorConfiguration configuration, SensorDeployDescriptor deployDescriptor, int sensorNo) {
        deployDescriptor.addProperty(SEND_INTERVAL, Integer.toString(configuration.getSendInterval()));
        deployDescriptor.addProperty(FILE_NAME, configuration.getFileName());
        deployDescriptor.addProperty(SEND_QUEUE_NAME_PROP, configuration.getBaseSendQueueName() + "_" + sensorNo);
        deployDescriptor.addProperty(RECEIVE_QUEUE_PROP, configuration.getBaseRecvQueueName() + "_" + sensorNo);
    }

    public static String readEntireFile(String filename) throws IOException {
        FileReader in = new FileReader(filename);
        StringBuilder contents = new StringBuilder();
        char[] buffer = new char[4096];
        int read = 0;
        do {
            contents.append(buffer, 0, read);
            read = in.read(buffer);
        } while (read >= 0);
        return contents.toString();
    }

    public static int getSendInterval(SensorContext context) {
        Object intervalProp = context.getProperty(SEND_INTERVAL);
        int interval = 100;
        if (intervalProp != null && intervalProp instanceof Integer) {
            interval = (Integer) intervalProp;
        }
        return interval;
    }

}

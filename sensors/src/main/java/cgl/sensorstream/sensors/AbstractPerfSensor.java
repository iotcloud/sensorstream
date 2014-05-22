package cgl.sensorstream.sensors;

import cgl.iotcloud.core.AbstractSensor;
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


    public static void deploy(String args[], List<String> sites, String className) {
        // read the configuration file
        Map conf = Utils.readConfig();
        SensorClient client;
        try {
            client = new SensorClient(conf);

            SensorDeployDescriptor deployDescriptor = new SensorDeployDescriptor("sensors-1.0-SNAPSHOT-jar-with-dependencies.jar", className);
            deployDescriptor.addDeploySites(sites);

            parseArgs(args, deployDescriptor);

            client.deploySensor(deployDescriptor);
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }

    public static void parseArgs(String []args, SensorDeployDescriptor descriptor) {
        Options options = new Options();
        options.addOption("t", true, "Time interval");
        options.addOption("f", true, "File name");

        CommandLineParser commandLineParser = new BasicParser();
        try {
            CommandLine cmd = commandLineParser.parse(options, args);

            String timeString = cmd.getOptionValue("t", "100");
            String fileName = cmd.getOptionValue("f");
            descriptor.addProperty(SEND_INTERVAL, timeString);
            descriptor.addProperty(FILE_NAME, fileName);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
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
}

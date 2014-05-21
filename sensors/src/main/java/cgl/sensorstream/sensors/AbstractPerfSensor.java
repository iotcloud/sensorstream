package cgl.sensorstream.sensors;

import cgl.iotcloud.core.AbstractSensor;
import cgl.iotcloud.core.sensorsite.SensorDeployDescriptor;
import org.apache.commons.cli.*;

import java.io.FileReader;
import java.io.IOException;

public abstract class AbstractPerfSensor extends AbstractSensor {
    public static final String SEND_INTERVAL = "send_interval";
    public static final String FILE_NAME = "file_name";

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

    private static String readEntireFile(String filename) throws IOException {
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

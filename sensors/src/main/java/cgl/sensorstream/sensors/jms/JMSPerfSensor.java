package cgl.sensorstream.sensors.jms;

import cgl.iotcloud.core.*;
import cgl.iotcloud.core.client.SensorClient;
import cgl.iotcloud.core.msg.SensorTextMessage;
import cgl.iotcloud.core.sensorsite.SensorDeployDescriptor;
import cgl.iotcloud.core.sensorsite.SiteContext;
import cgl.iotcloud.core.transport.Channel;
import cgl.iotcloud.core.transport.Direction;
import cgl.iotcloud.core.transport.MessageConverter;
import org.apache.commons.cli.*;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class JMSPerfSensor extends AbstractSensor {
    public static final String SEND_QUEUE_NAME_PROP = "send_queue";
    public static final String RECEIVE_QUEUE_PROP = "recv_queue";

    public static final String SEND_INTERVAL = "send_interval";
    public static final String FILE_NAME = "file_name";

    private static Logger LOG = LoggerFactory.getLogger(JMSPerfSensor.class);

    private SensorContext context;

    @Override
    public Configurator getConfigurator(Map conf) {
        return new ChatConfigurator();
    }

    @Override
    public void open(SensorContext context) { Object intervalProp = context.getProperty(SEND_INTERVAL);
        int interval = 100;
        if (intervalProp != null && intervalProp instanceof Integer) {
            interval = (Integer) intervalProp;
        }
        String fileName = context.getProperty(FILE_NAME).toString();

        this.context = context;

        final Channel sendChannel = context.getChannel("jms", "sender");
        final Channel receiveChannel = context.getChannel("jms", "receiver");

        final String content;
        try {
            content = readEntireFile(fileName);
        } catch (IOException e) {
            String s = "Failed to read the file";
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }

        startChannel(sendChannel, new MessageSender() {
            @Override
            public boolean loop(BlockingQueue queue) {
                try {
                    queue.put(new SensorTextMessage(content));
                } catch (InterruptedException e) {
                    LOG.error("Error", e);
                }
                return true;
            }
        }, interval);

        startChannel(receiveChannel, new MessageReceiver() {
            @Override
            public void onMessage(Object message) {
                if (message instanceof SensorTextMessage) {
                    System.out.println(((SensorTextMessage) message).getText());
                } else {
                    System.out.println("Unexpected message");
                }
            }
        });

        LOG.info("Received open request {}", this.context.getId());
    }

    @Override
    public void close() {
        if (context != null) {
            for (List<Channel> cs : context.getChannels().values()) {
                for (Channel c : cs) {
                    c.close();
                }
            }
        }
    }

    private class ChatConfigurator implements Configurator {
        @Override
        public SensorContext configure(SiteContext siteContext, Map conf) {
            SensorContext context = new SensorContext(new SensorId("chat", "general"));

            String sendQueue = (String) conf.get(SEND_QUEUE_NAME_PROP);
            String recvQueue = (String) conf.get(RECEIVE_QUEUE_PROP);
            String fileName = (String) conf.get(FILE_NAME);

            String sendInterval = (String) conf.get(SEND_INTERVAL);
            int interval = Integer.parseInt(sendInterval);
            context.addProperty(SEND_INTERVAL, interval);
            context.addProperty(FILE_NAME, fileName);

            BlockingQueue inMassages = new ArrayBlockingQueue(1024);
            BlockingQueue outMassages = new ArrayBlockingQueue(1024);
            Map properties = new HashMap();
            properties.put(Configuration.CHANNEL_JMS_IS_QUEUE, "false");
            properties.put(Configuration.CHANNEL_JMS_DESTINATION, recvQueue);
            Channel receiveChannel = new Channel("receiver", Direction.IN, inMassages, outMassages, new JMSToTextMessageConverter());
            receiveChannel.addProperties(properties);
            context.addChannel("jms", receiveChannel);

            inMassages = new ArrayBlockingQueue(1024);
            outMassages = new ArrayBlockingQueue(1024);
            properties = new HashMap();
            properties.put(Configuration.CHANNEL_JMS_IS_QUEUE, "false");
            properties.put(Configuration.CHANNEL_JMS_DESTINATION, sendQueue);
            Channel sendChannel = new Channel("sender", Direction.OUT, inMassages, outMassages, new TextToJMSMessageConverter());
            sendChannel.addProperties(properties);
            context.addChannel("jms", sendChannel);

            return context;
        }
    }

    private class JMSToTextMessageConverter implements MessageConverter {

        @Override
        public Object convert(Object input, Object context) {
            if (input instanceof TextMessage) {
                try {
                    return new SensorTextMessage(((TextMessage) input).getText());
                } catch (JMSException e) {
                    LOG.error("Failed to convert JMS message to SensorTextMessage", e);
                }
            }
            return null;
        }
    }

    private class TextToJMSMessageConverter implements MessageConverter {
        @Override
        public Object convert(Object input, Object context) {
            if (context instanceof Session && input instanceof SensorTextMessage) {
                try {
                    TextMessage message = ((Session) context).createTextMessage();
                    message.setText(((SensorTextMessage) input).getText());

                    return message;
                } catch (JMSException e) {
                    LOG.error("Failed to convert SensorTextMessage to JMS message", e);
                }
            }
            return null;
        }
    }

    public static void main(String[] args) {
        // read the configuration file
        Map conf = Utils.readConfig();
        SensorClient client;
        try {
            client = new SensorClient(conf);

            List<String> sites = new ArrayList<String>();
            sites.add("local");

            SensorDeployDescriptor deployDescriptor = new SensorDeployDescriptor("sensors-1.0-SNAPSHOT.jar", "cgl.sensorstream.sensors.jms.JMSPerfSensor");
            deployDescriptor.addDeploySites(sites);

            client.deploySensor(deployDescriptor);
        } catch (TTransportException e) {
            e.printStackTrace();
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
}

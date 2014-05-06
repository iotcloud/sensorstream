package cgl.sensorstream.sensors.rabbitmq;

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

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class RMQPerfSensor extends AbstractSensor {
    public static final String EXCHANGE_NAME_PROP = "exchange";
    public static final String SEND_QUEUE_NAME_PROP = "send_queue";
    public static final String RECEIVE_QUEUE_PROP = "recv_queue";
    public static final String ROUTING_KEY_PROP = "routing_key";

    public static final String SEND_INTERVAL = "send_interval";
    public static final String FILE_NAME = "file_name";

    private static Logger LOG = LoggerFactory.getLogger(RMQPerfSensor.class);

    private SensorContext context;

    @Override
    public Configurator getConfigurator(Map conf) {
        return new RabbitConfigurator();
    }

    @Override
    public void open(SensorContext context) {
        this.context = context;

        Object intervalProp = context.getProperty(SEND_INTERVAL);
        int interval = 100;
        if (intervalProp != null && intervalProp instanceof Integer) {
            interval = (Integer) intervalProp;
        }
        String fileName = context.getProperty(FILE_NAME).toString();

        final Channel sendChannel = context.getChannel("rabbitmq", "sender");
        final Channel receiveChannel = context.getChannel("rabbitmq", "receiver");
        final String content;
        try {
            content = readEntireFile(fileName);
        } catch (IOException e) {
            String s = "Failed to read the file";
            LOG.error(s, e);
            throw new RuntimeException(s, e);
        }

        startSend(sendChannel, new MessageSender() {
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

//        startChannel(receiveChannel, new MessageReceiver() {
//            @Override
//            public void onMessage(Object message) {
//                if (message instanceof SensorTextMessage) {
//                    System.out.println(((SensorTextMessage) message).getText());
//                } else {
//                    System.out.println("Unexpected message");
//                }
//            }
//        });

        LOG.info("Received open request {}", this.context.getId());
    }

    private class RabbitConfigurator extends AbstractConfigurator {
        @Override
        public SensorContext configure(SiteContext siteContext, Map conf) {
            SensorContext context = new SensorContext(new SensorId("rabbitChat", "general"));
            String exchange = (String) conf.get(EXCHANGE_NAME_PROP);
            String sendQueue = (String) conf.get(SEND_QUEUE_NAME_PROP);
            String recvQueue = (String) conf.get(RECEIVE_QUEUE_PROP);
            String routingKey = (String) conf.get(ROUTING_KEY_PROP);
            String fileName = (String) conf.get(FILE_NAME);

            String sendInterval = (String) conf.get(SEND_INTERVAL);
            int interval = Integer.parseInt(sendInterval);
            context.addProperty(SEND_INTERVAL, interval);
            context.addProperty(FILE_NAME, fileName);

            Map sendProps = new HashMap();
            sendProps.put("exchange", exchange);
            sendProps.put("routingKey", routingKey);
            sendProps.put("queueName", sendQueue);
            Channel sendChannel = createChannel("sender", sendProps, Direction.OUT, 1024, new TextToByteConverter());

            Map receiveProps = new HashMap();
            receiveProps.put("queueName", recvQueue);
            Channel receiveChannel = createChannel("receiver", receiveProps, Direction.IN, 1024, new ByteToTextConverter());

            context.addChannel("rabbitmq", sendChannel);
            // context.addChannel("rabbitmq", receiveChannel);

            return context;
        }
    }

    private class ByteToTextConverter implements MessageConverter {
        public Object convert(Object input, Object context) {
            if (input instanceof byte[]) {
                ByteArrayInputStream in = new ByteArrayInputStream((byte[]) input);
                ObjectInputStream is;
                try {
                    is = new ObjectInputStream(in);
                    return is.readObject();
                } catch (Exception e) {
                    LOG.error("Error occurred while converting byte to text", e);
                }
            }
            return null;
        }
    }

    private class TextToByteConverter implements MessageConverter {
        @Override
        public Object convert(Object input, Object context) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = null;
            try {
                out = new ObjectOutputStream(bos);
                out.writeObject(input);
                return bos.toByteArray();
            } catch (IOException e) {
                LOG.error("Error occurred while converting text to byte", e);
            } finally {
                try {
                    if (out != null) {
                        out.close();
                    }
                } catch (IOException ignore) {
                    // ignore close exception
                }
                try {
                    bos.close();
                } catch (IOException ignore) {
                    // ignore close exception
                }
            }
            return null;
        }
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

    public static void main(String[] args) {
        // read the configuration file
        Map conf = Utils.readConfig();
        SensorClient client;
        try {
            client = new SensorClient(conf);
            SensorDeployDescriptor deployDescriptor = new SensorDeployDescriptor("sensors-1.0-SNAPSHOT-jar-with-dependencies.jar",
                    "cgl.sensorstream.sensors.rabbitmq.RMQPerfSensor");
            parseArgs(args, deployDescriptor);

            deployDescriptor.addProperty(EXCHANGE_NAME_PROP, "testExchange");
            deployDescriptor.addProperty(ROUTING_KEY_PROP, "testRoute");
            deployDescriptor.addProperty(SEND_QUEUE_NAME_PROP, "send");
            deployDescriptor.addProperty(RECEIVE_QUEUE_PROP, "receive");

            List<String> sites = new ArrayList<String>();
            sites.add("local-1");
            sites.add("local-2");
            deployDescriptor.addDeploySites(sites);

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

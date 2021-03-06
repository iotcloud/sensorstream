package cgl.sensorstream.sensors.kafka;

import cgl.iotcloud.core.*;
import cgl.iotcloud.core.msg.SensorTextMessage;
import cgl.iotcloud.core.sensorsite.SiteContext;
import cgl.iotcloud.core.transport.Channel;
import cgl.iotcloud.core.transport.Direction;
import cgl.iotcloud.core.transport.IdentityConverter;
import cgl.iotcloud.core.transport.MessageConverter;
import cgl.iotcloud.transport.kafka.KafkaMessage;
import cgl.sensorstream.sensors.AbstractPerfSensor;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class KafkaPerfSensor extends AbstractPerfSensor {
    private static Logger LOG = LoggerFactory.getLogger(KafkaPerfSensor.class);

    private SensorContext context;

    public Configurator getConfigurator(Map map) {
        return new KestrelConfigurator();
    }

    @Override
    public void open(SensorContext context) {
        this.context = context;

        int interval = getSendInterval(context);
        String fileName = context.getProperty(FILE_NAME).toString();
        final Channel sendChannel = context.getChannel("kafka", "sender");
        final Channel receiveChannel = context.getChannel("kafka", "receiver");

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

        startListen(receiveChannel, new MessageReceiver() {
            @Override
            public void onMessage(Object message) {
                if (message instanceof KafkaMessage) {
                    KafkaMessage envelope = (KafkaMessage) message;
                    byte []body = envelope.getData();
                    String bodyS = new String(body);
                    BufferedReader reader = new BufferedReader(new StringReader(bodyS));
                    String timeStampS = null;
                    try {
                        timeStampS = reader.readLine();
                    } catch (IOException e) {
                        LOG.error("Error occurred while reading the bytes", e);
                    }
                    try {
                        Long timeStamp = Long.parseLong(timeStampS);
                        long currentTime = System.currentTimeMillis();
                        calculateAverage(currentTime - timeStamp);
                        LOG.info("latency: " + averageLatency + " initial time: " + timeStamp + " current: " + currentTime + " difference:" + (currentTime - timeStamp));
                    } catch (NumberFormatException e) {
                        LOG.warn("Unexpected message");
                    }
                } else {
                    LOG.error("Unexpected message");
                }
            }
        });

        LOG.info("Received open request {}", this.context.getId());
    }

    private class KestrelOutMessageConverter implements MessageConverter {
        @Override
        public Object convert(Object o, Object o1) {
            long currentTime = System.currentTimeMillis();
            String send = currentTime + "\r\n" + o.toString();
            KafkaMessage message = new KafkaMessage(null, 0, send.getBytes());
            message.setKey("key");
            return message;
        }
    }

    private class KestrelConfigurator extends AbstractConfigurator {
        @Override
        public SensorContext configure(SiteContext siteContext, Map conf) {
            String sendQueue = (String) conf.get(SEND_QUEUE_NAME_PROP);
            String recvQueue = (String) conf.get(RECEIVE_QUEUE_PROP);
            String fileName = (String) conf.get(FILE_NAME);
            String sensorName = (String) conf.get(SENSOR_NAME);
            String server = (String) conf.get(SERVER);

            SensorContext context = new SensorContext(new SensorId(sensorName, "general"));

            String sendInterval = (String) conf.get(SEND_INTERVAL);
            int interval = Integer.parseInt(sendInterval);
            context.addProperty(SEND_INTERVAL, interval);
            context.addProperty(FILE_NAME, fileName);

            Map sendProps = new HashMap();
            sendProps.put("topic", sendQueue);
            sendProps.put("server", server);
            Channel sendChannel = createChannel("sender", sendProps, Direction.OUT, 1024, new KestrelOutMessageConverter());

            Map receiveProps = new HashMap();
            receiveProps.put("topic", recvQueue);
            receiveProps.put("server", server);
            receiveProps.put("partition", 0);
            Channel receiveChannel = createChannel("receiver", receiveProps, Direction.IN, 1024, new IdentityConverter());

            context.addChannel("kafka", sendChannel);
            context.addChannel("kafka", receiveChannel);

            return context;
        }
    }


    public static void main(String[] args) {
        List<String> sites = new ArrayList<String>();
        sites.add("local-1");
        sites.add("local-2");
//        sites.add("local-2");
        try {
            deploy(args, sites, KafkaPerfSensor.class.getCanonicalName());
        } catch (TTransportException e) {
            LOG.error("Error deploying the sensor", e);
        }
    }
}

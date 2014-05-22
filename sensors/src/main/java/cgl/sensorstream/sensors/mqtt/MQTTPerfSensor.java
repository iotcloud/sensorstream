package cgl.sensorstream.sensors.mqtt;

import cgl.iotcloud.core.*;
import cgl.iotcloud.core.client.SensorClient;
import cgl.iotcloud.core.msg.SensorTextMessage;
import cgl.iotcloud.core.sensorsite.SensorDeployDescriptor;
import cgl.iotcloud.core.sensorsite.SiteContext;
import cgl.iotcloud.core.transport.Channel;
import cgl.iotcloud.core.transport.Direction;
import cgl.iotcloud.core.transport.IdentityConverter;
import cgl.iotcloud.core.transport.MessageConverter;
import cgl.iotcloud.transport.mqtt.MQTTMessage;
import cgl.sensorstream.sensors.AbstractPerfSensor;
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

public class MQTTPerfSensor extends AbstractPerfSensor {
    private static Logger LOG = LoggerFactory.getLogger(MQTTPerfSensor.class);

    private SensorContext context;

    @Override
    public cgl.iotcloud.core.Configurator getConfigurator(Map conf) {
        return new MQTTConfigurator();
    }

    @Override
    public void open(SensorContext context) {
        this.context = context;

        int interval = getSendInterval(context);
        String fileName = context.getProperty(FILE_NAME).toString();
        final Channel sendChannel = context.getChannel("mqtt", "sender");
        final Channel receiveChannel = context.getChannel("mqtt", "receiver");

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
                if (message instanceof MQTTMessage) {
                    MQTTMessage envelope = (MQTTMessage) message;
                    byte []body = envelope.getBody().toByteArray();
                    String bodyS = new String(body);
                    BufferedReader reader = new BufferedReader(new StringReader(bodyS));
                    String timeStampS = null;
                    try {
                        timeStampS = reader.readLine();
                    } catch (IOException e) {
                        LOG.error("Error occurred while reading the bytes", e);
                    }
                    Long timeStamp = Long.parseLong(timeStampS);
                    long currentTime = System.currentTimeMillis();
                    LOG.info("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
                } else {
                    LOG.error("Unexpected message");
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

    private class MQTTConfigurator extends AbstractConfigurator {
        @Override
        public SensorContext configure(SiteContext siteContext, Map conf) {
            SensorContext context = new SensorContext(new SensorId("mqttPerf", "general"));
            String sendQueue = (String) conf.get(SEND_QUEUE_NAME_PROP);
            String recvQueue = (String) conf.get(RECEIVE_QUEUE_PROP);
            String fileName = (String) conf.get(FILE_NAME);

            String sendInterval = (String) conf.get(SEND_INTERVAL);
            int interval = Integer.parseInt(sendInterval);
            context.addProperty(SEND_INTERVAL, interval);
            context.addProperty(FILE_NAME, fileName);

            Map sendProps = new HashMap();
            sendProps.put("queueName", sendQueue);
            Channel sendChannel = createChannel("sender", sendProps, Direction.OUT, 1024, new MQTTOutMessageConverter());

            Map receiveProps = new HashMap();
            receiveProps.put("queueName", recvQueue);
            Channel receiveChannel = createChannel("receiver", receiveProps, Direction.IN, 1024, new IdentityConverter());

            context.addChannel("mqtt", sendChannel);
            context.addChannel("rabbitmq", receiveChannel);

            return context;
        }
    }

    private class MQTTOutMessageConverter implements MessageConverter {
        @Override
        public Object convert(Object o, Object o1) {
            long currentTime = System.currentTimeMillis();
            String send = currentTime + "\r\n" + o.toString();
            return send.getBytes();
        }
    }

    public static void main(String[] args) {
        List<String> sites = new ArrayList<String>();
        sites.add("local-1");
        sites.add("local-2");
        deploy(args, sites, MQTTPerfSensor.class.getCanonicalName());
    }
}

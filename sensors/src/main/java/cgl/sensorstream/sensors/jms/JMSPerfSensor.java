package cgl.sensorstream.sensors.jms;

import cgl.iotcloud.core.*;
import cgl.iotcloud.core.msg.SensorTextMessage;
import cgl.iotcloud.core.sensorsite.SiteContext;
import cgl.iotcloud.core.transport.Channel;
import cgl.iotcloud.core.transport.Direction;
import cgl.iotcloud.core.transport.IdentityConverter;
import cgl.iotcloud.core.transport.MessageConverter;
import cgl.sensorstream.sensors.AbstractPerfSensor;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class JMSPerfSensor extends AbstractPerfSensor {
    private static Logger LOG = LoggerFactory.getLogger(JMSPerfSensor.class);

    private SensorContext context;

    @Override
    public Configurator getConfigurator(Map conf) {
        return new JMSConfigurator();
    }

    @Override
    public void open(SensorContext context) {
        Object intervalProp = context.getProperty(SEND_INTERVAL);
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
                if (message instanceof TextMessage) {
                    long currentTime = System.currentTimeMillis();
                    try {
                        String bodyS = ((TextMessage) message).getText();
                        BufferedReader reader = new BufferedReader(new StringReader(bodyS));
                        String timeStampS = null;
                        try {
                            timeStampS = reader.readLine();
                        } catch (IOException e) {
                            LOG.error("Error occurred while reading the bytes", e);
                        }
                        Long timeStamp = Long.parseLong(timeStampS);
                        calculateAverage(currentTime - timeStamp);
                        LOG.info("latency: " + averageLatency + " initial time: " + timeStamp + " current: " + currentTime);
                    } catch (JMSException e) {
                        LOG.error("Error", e);
                    }
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

    private class JMSConfigurator extends AbstractConfigurator {
        @Override
        public SensorContext configure(SiteContext siteContext, Map conf) {
            String sendQueue = (String) conf.get(SEND_QUEUE_NAME_PROP);
            String recvQueue = (String) conf.get(RECEIVE_QUEUE_PROP);
            String fileName = (String) conf.get(FILE_NAME);
            String sensorName = (String) conf.get(SENSOR_NAME);

            SensorContext context = new SensorContext(new SensorId(sensorName, "general"));

            String sendInterval = (String) conf.get(SEND_INTERVAL);
            int interval = Integer.parseInt(sendInterval);
            context.addProperty(SEND_INTERVAL, interval);
            context.addProperty(FILE_NAME, fileName);

            Map properties = new HashMap();
            properties.put(Configuration.CHANNEL_JMS_IS_QUEUE, "false");
            properties.put(Configuration.CHANNEL_JMS_DESTINATION, recvQueue);
            Channel receiveChannel = createChannel("receiver", properties, Direction.IN, 1024, new IdentityConverter());

            properties = new HashMap();
            properties.put(Configuration.CHANNEL_JMS_IS_QUEUE, "false");
            properties.put(Configuration.CHANNEL_JMS_DESTINATION, sendQueue);
            Channel sendChannel = createChannel("sender", properties, Direction.OUT, 1024, new TextToJMSMessageConverter());

            context.addChannel("jms", receiveChannel);
            context.addChannel("jms", sendChannel);

            return context;
        }
    }

    private class TextToJMSMessageConverter implements MessageConverter {
        @Override
        public Object convert(Object input, Object context) {
            if (context instanceof Session && input instanceof SensorTextMessage) {
                try {
                    TextMessage message = ((Session) context).createTextMessage();
                    long currentTime = System.currentTimeMillis();
                    String send = currentTime + "\r\n" + ((SensorTextMessage) input).getText();
                    message.setText(send);
                    return message;
                } catch (JMSException e) {
                    LOG.error("Failed to convert SensorTextMessage to JMS message", e);
                }
            }
            return null;
        }
    }

    public static void main(String[] args) {
        List<String> sites = new ArrayList<String>();
        sites.add("local");
        try {
            deploy(args, sites, JMSPerfSensor.class.getCanonicalName());
        } catch (TTransportException e) {
            LOG.error("Error deploying the sensor", e);
        }
    }
}

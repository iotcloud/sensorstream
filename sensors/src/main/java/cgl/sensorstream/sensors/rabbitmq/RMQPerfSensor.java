package cgl.sensorstream.sensors.rabbitmq;

import cgl.iotcloud.core.*;
import cgl.iotcloud.core.sensorsite.SiteContext;
import cgl.iotcloud.core.transport.Channel;
import cgl.iotcloud.core.transport.Direction;
import cgl.iotcloud.core.transport.IdentityConverter;
import cgl.iotcloud.core.transport.MessageConverter;
import cgl.iotcloud.transport.rabbitmq.RabbitMQMessage;
import cgl.iotcloud.transport.rabbitmq.RabbitMQSender;
import cgl.sensorstream.sensors.AbstractPerfSensor;
import com.rabbitmq.client.AMQP;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.TextMessage;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

public class RMQPerfSensor extends AbstractPerfSensor {
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
                    queue.put(new FileContentMessage(content));
                } catch (InterruptedException e) {
                    LOG.error("Error", e);
                }
                return true;
            }
        }, interval);

        startListen(receiveChannel, new MessageReceiver() {
            @Override
            public void onMessage(Object message) {
                if (message instanceof RabbitMQMessage) {
                    byte []body= ((RabbitMQMessage) message).getBody();
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
                    calculateAverage(currentTime - timeStamp);
                    LOG.info("latency: " + averageLatency + " initial time: " + timeStamp + " current: " + currentTime);
                } else {
                    System.out.println("Unexpected message");
                }
            }
        });

        LOG.info("Received open request {}", this.context.getId());
    }

    private class RabbitConfigurator extends AbstractConfigurator {
        @Override
        public SensorContext configure(SiteContext siteContext, Map conf) {
            SensorContext context = new SensorContext(new SensorId("rabbitChat", "general"));
            String exchange = (String) conf.get(SEND_EXCHANGE_NAME_PROP);
            String sendQueue = (String) conf.get(SEND_QUEUE_NAME_PROP);

            String recvExchange = (String) conf.get(RECV_EXCHANGE_NAME_PROP);

            String recvQueue = (String) conf.get(RECEIVE_QUEUE_PROP);
            String routingKey = (String) conf.get(SEND_ROUTING_KEY_PROP);
            String recvRoutingKey = (String) conf.get(RECV_ROUTING_KEY_PROP);
            String fileName = (String) conf.get(FILE_NAME);

            String sendInterval = (String) conf.get(SEND_INTERVAL);
            int interval = Integer.parseInt(sendInterval);
            context.addProperty(SEND_INTERVAL, interval);
            context.addProperty(FILE_NAME, fileName);

            Map sendProps = new HashMap();
            sendProps.put("exchange", exchange);
            sendProps.put("routingKey", routingKey);
            sendProps.put("queueName", sendQueue);
            Channel sendChannel = createChannel("sender", sendProps, Direction.OUT, 1024, new FileContentToRabbitMessageConverter());

            Map receiveProps = new HashMap();
            receiveProps.put("queueName", recvQueue);
            receiveProps.put("exchange", recvExchange);
            receiveProps.put("routingKey", recvRoutingKey);
            Channel receiveChannel = createChannel("receiver", receiveProps, Direction.IN, 1024, new IdentityConverter());

            context.addChannel("rabbitmq", sendChannel);
            context.addChannel("rabbitmq", receiveChannel);

            return context;
        }
    }

    private class FileContentMessage {
        private String content;

        private FileContentMessage(String content) {
            this.content = content;
        }

        public String getContent() {
            return content;
        }
    }

    private class FileContentToRabbitMessageConverter implements MessageConverter {
        @Override
        public Object convert(Object input, Object context) {
            if (input instanceof FileContentMessage) {
                long currentTime = System.currentTimeMillis();
                String send = currentTime + "\r\n" + ((FileContentMessage) input).getContent();
                RabbitMQMessage message = new RabbitMQMessage(null, send.getBytes());
                return message;
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
        List<String> sites = new ArrayList<String>();
        sites.add("local");
//        sites.add("local-2");
        try {
            deploy(args, sites, RMQPerfSensor.class.getCanonicalName());
        } catch (TTransportException e) {
            LOG.error("Error deploying the sensor", e);
        }
    }
}

package cgl.sensorstream.storm.perf;

import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.ss.rabbitmq.*;
import com.ss.rabbitmq.bolt.RabbitMQBolt;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Serializable;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class RRPerfTopology extends AbstractPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        TopologyConfiguration configuration = parseArgs(args[0], null);

        ErrorReporter r = new ErrorReporter() {
            @Override
            public void reportError(Throwable t) {
                t.printStackTrace();
            }
        };

        int i = 0;
        for (Endpoint ip : configuration.getEndpoints()) {
            for (String iot : ip.getIotServers()) {
                RabbitMQSpout spout = new RabbitMQSpout(new SpoutConfigurator(iot + "." + configuration.getRecv(), ip.getUrl(), ip.getProperties().get("exchange")), r);
                RabbitMQBolt bolt = new RabbitMQBolt(new BoltConfigurator(iot + "." +  configuration.getSend(), ip.getUrl(), ip.getProperties().get("exchange")), r);
                builder.setSpout("rabbit_spout_" + i, spout, configuration.getParallism());
                builder.setBolt("rabbit_bolt_" + i, bolt, configuration.getParallism()).shuffleGrouping("rabbit_spout_" + i);
                i++;
            }
        }
        submit(args, "rabbitTest", builder, configuration);
    }

    private static class TimeStampMessageBuilder implements MessageBuilder {
        @Override
        public List<Object> deSerialize(RabbitMQMessage message) {
            byte []body = message.getBody();
            String bodyS = new String(body);
            BufferedReader reader = new BufferedReader(new StringReader(bodyS));
            String timeStampS = null;
            try {
                timeStampS = reader.readLine();
            } catch (IOException e) {
                e.printStackTrace();
            }
            Long timeStamp = Long.parseLong(timeStampS);
            long currentTime = System.currentTimeMillis();
            System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
            List<Object> tuples = new ArrayList<Object>();

            SendMessage sendMessage = new SendMessage(message.getQueue(), message.getBody());
            tuples.add(sendMessage);
            return tuples;
        }

        @Override
        public RabbitMQMessage serialize(Tuple tuple) {
            Object message = tuple.getValue(0);
            if (message instanceof  SendMessage){
                RabbitMQMessage rrMessage = new RabbitMQMessage(((SendMessage) message).getQueue(), null, null, null, ((SendMessage) message).getContent());
                return rrMessage;
            }
            return null;
        }
    }

    private static class SendMessage implements Serializable {
        private String queue;

        private byte[] content;

        private SendMessage(String queue, byte[] content) {
            this.queue = queue;
            this.content = content;
        }

        public String getQueue() {
            return queue;
        }

        public byte[] getContent() {
            return content;
        }
    }

    private static class SpoutConfigurator implements RabbitMQConfigurator {
        private String url = "amqp://localhost:5672";

        private String ip;

        private String recv;

        private String exchange;

        private SpoutConfigurator(String recv, String ip, String exchange) {
            this.recv = recv;
            this.ip = ip;
            this.exchange = exchange;
        }

        @Override
        public String getURL() {
            return ip;
        }

        @Override
        public boolean isAutoAcking() {
            return true;
        }

        @Override
        public int getPrefetchCount() {
            return 1024;
        }

        @Override
        public boolean isReQueueOnFail() {
            return false;
        }

        @Override
        public String getConsumerTag() {
            return "sender";
        }

        @Override
        public List<RabbitMQDestination> getQueueName() {
            List<RabbitMQDestination> list = new ArrayList<RabbitMQDestination>();
            list.add(new RabbitMQDestination(recv, exchange, recv));
            return list;
        }

        @Override
        public MessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("time1"));
        }

        @Override
        public int queueSize() {
            return 1024;
        }

        @Override
        public RabbitMQDestinationSelector getDestinationSelector() {
            return null;
        }
    }

    private static class BoltConfigurator implements RabbitMQConfigurator {
        private String url = "amqp://localhost:5672";

        private String ip;

        private String send;

        private String exchange;

        private BoltConfigurator(String send, String ip, String exchange) {
            this.ip = ip;
            this.send = send;
            this.exchange = exchange;
        }

        @Override
        public String getURL() {
            return ip;
        }

        @Override
        public boolean isAutoAcking() {
            return false;
        }

        @Override
        public int getPrefetchCount() {
            return 1024;
        }

        @Override
        public boolean isReQueueOnFail() {
            return false;
        }

        @Override
        public String getConsumerTag() {
            return "sender";
        }

        @Override
        public List<RabbitMQDestination> getQueueName() {
            List<RabbitMQDestination> list = new ArrayList<RabbitMQDestination>();
            list.add(new RabbitMQDestination(send, exchange, send));
            return list;
        }

        @Override
        public MessageBuilder getMessageBuilder() {
            return new TimeStampMessageBuilder();
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("time1"));
        }

        @Override
        public int queueSize() {
            return 1024;
        }

        @Override
        public RabbitMQDestinationSelector getDestinationSelector() {
            return new RabbitMQDestinationSelector() {
                @Override
                public String select(Tuple message) {
                    return send;
                }
            };
        }
    }
}

package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import com.ss.rabbitmq.ErrorReporter;
import com.ss.rabbitmq.MessageBuilder;
import com.ss.rabbitmq.RabbitMQConfigurator;
import com.ss.rabbitmq.RabbitMQSpout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class RRPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        ErrorReporter r = new ErrorReporter() {
            @Override
            public void reportError(Throwable t) {
                t.printStackTrace();
            }
        };

        RabbitMQSpout spout = new RabbitMQSpout(new Configurator(), r);
        builder.setSpout("word", spout, 1);
        builder.setBolt("time1", new PerfAggrBolt(), 2).shuffleGrouping("word");

        Config conf = new Config();
        if (args != null && args.length > 0) {
            conf.setNumWorkers(6);
            StormSubmitter.submitTopology("test", conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Thread.sleep(60000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    private static class TimeStampMessageBuilder implements MessageBuilder {
        @Override
        public List<Object> deSerialize(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) {
            Map<String, Object> headers = properties.getHeaders();
            Long timeStamp = (Long) headers.get("time");
            long currentTime = System.currentTimeMillis();

            System.out.println("latency: " + (currentTime - timeStamp) + " initial time: " + timeStamp + " current: " + currentTime);
            List<Object> tuples = new ArrayList<Object>();
            tuples.add(new Long((currentTime - timeStamp)));
            return tuples;
        }
    }

    private static class Configurator implements RabbitMQConfigurator {
        private String url = "amqp://localhost:5672";

        private String queueName = "send";

        @Override
        public String getURL() {
            return url;
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
        public List<String> getQueueName() {
            return new ArrayList<String>(Arrays.asList(queueName));
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
    }
}

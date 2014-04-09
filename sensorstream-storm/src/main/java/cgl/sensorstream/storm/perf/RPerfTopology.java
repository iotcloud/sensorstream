package cgl.sensorstream.storm.perf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.Scheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import io.latent.storm.rabbitmq.RabbitMQMessageScheme;
import io.latent.storm.rabbitmq.RabbitMQSpout;

import java.util.ArrayList;
import java.util.List;

public class RPerfTopology {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        RabbitMQSpout spout = new RabbitMQSpout(new RMessageScheme(new RScheme(), "e", "p"));
        builder.setSpout("word", spout, 2);
        builder.setBolt("time1", new PerfAggrBolt(), 2).shuffleGrouping("word");

        Config conf = new Config();
        conf.put("rabbitmq.uri", "amqp://localhost:5672");
        conf.put("rabbitmq.prefetchCount", "2");
        conf.put("rabbitmq.queueName", "send");
        conf.put("rabbitmq.requeueOnFail", "false");

        if (args != null && args.length > 0) {
            conf.setNumWorkers(6);
            StormSubmitter.submitTopology("perfr", conf, builder.createTopology());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("perfr", conf, builder.createTopology());
            Thread.sleep(60000);
            cluster.killTopology("perfr");
            cluster.shutdown();
        }
    }

    public static class RScheme implements Scheme {
        @Override
        public List<Object> deserialize(byte[] bytes) {
            return null;
        }

        @Override
        public Fields getOutputFields() {
            return new Fields("time1");
        }
    }

    public static class RMessageScheme extends RabbitMQMessageScheme {
        public RMessageScheme(Scheme payloadScheme, String envelopeFieldName, String propertiesFieldName) {
            super(payloadScheme, envelopeFieldName, propertiesFieldName);
        }

        @Override
        public List<Object> deserialize(io.latent.storm.rabbitmq.Message message) {
            io.latent.storm.rabbitmq.Message.DeliveredMessage dm = (io.latent.storm.rabbitmq.Message.DeliveredMessage)message;
            List<Object> list = new ArrayList<Object>();
            list.add(System.currentTimeMillis() - dm.getTimestamp().getTime());
//            long k = 10L;
//            list.add(k);
            list.add("");
            list.add("");
            return list;
        }
    }
}

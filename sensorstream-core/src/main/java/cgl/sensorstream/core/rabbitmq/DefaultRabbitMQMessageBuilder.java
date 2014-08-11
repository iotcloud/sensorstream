package cgl.sensorstream.core.rabbitmq;

import backtype.storm.tuple.Tuple;
import cgl.iotcloud.core.transport.TransportConstants;
import com.rabbitmq.client.AMQP;
import com.ss.commons.MessageBuilder;
import com.ss.commons.MessageContext;
import com.ss.rabbitmq.RabbitMQMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultRabbitMQMessageBuilder implements MessageBuilder {
    private static Logger LOG = LoggerFactory.getLogger(DefaultRabbitMQMessageBuilder.class);

    public List<Object> deSerialize(Object o) {
        List<Object> tuples = new ArrayList<Object>();
        if (o instanceof MessageContext) {
            if (((MessageContext) o).getMessage() instanceof RabbitMQMessage) {
                RabbitMQMessage rabbitMQMessage = (RabbitMQMessage) ((MessageContext) o).getMessage();

                AMQP.BasicProperties properties = rabbitMQMessage.getProperties();
                Object time = null;
                Object sensorId = null;
                if (properties != null && properties.getHeaders() != null) {
                    sensorId = properties.getHeaders().get(TransportConstants.SENSOR_ID);
                    time = properties.getHeaders().get("time");
                }


                tuples.add(rabbitMQMessage.getBody());
                if (sensorId != null) {
                    tuples.add(sensorId);
                }
                if (time !=  null) {
                    tuples.add(time);
                }
            }
        }
        return tuples;
    }

    @Override
    public Object serialize(Tuple tuple, Object o) {
        byte []body = (byte[]) tuple.getValueByField("body");
        String sensorId = (String) tuple.getValueByField(TransportConstants.SENSOR_ID);
        String time = (String) tuple.getValueByField("time");
        Map<String, Object> props = new HashMap<String, Object>();
        props.put(TransportConstants.SENSOR_ID, sensorId);
        props.put("time", time);
        // System.out.println("Sending message" + motion);
        return new RabbitMQMessage(null, null, null, new AMQP.BasicProperties.Builder().headers(props).build(), body);
    }
}

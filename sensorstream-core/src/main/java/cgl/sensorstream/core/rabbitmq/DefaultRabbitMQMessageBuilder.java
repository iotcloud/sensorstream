package cgl.sensorstream.core.rabbitmq;

import backtype.storm.tuple.Tuple;
import cgl.iotcloud.core.transport.TransportConstants;
import com.rabbitmq.client.AMQP;
import com.ss.commons.MessageBuilder;
import com.ss.commons.MessageContext;
import com.ss.rabbitmq.RabbitMQMessage;

import java.util.ArrayList;
import java.util.List;

public class DefaultRabbitMQMessageBuilder implements MessageBuilder {
    @Override
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
        return null;
    }
}

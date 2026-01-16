package me.zonglun.seckilling.rabbitmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Service;

/**
 * @author : Administrator
 * @create 2018-05-04 19:35
 */
@Service
public class MQReciver {

    private static Logger log = LoggerFactory.getLogger(MQReciver.class);


    @RabbitListener(queues=MQconfig.QUEUE)
    public void receive(String message) {
			log.info("receive message:"+message);
    }

		@RabbitListener(queues=MQconfig.TOPIC_QUEUE1)
		public void receiveTopic1(String message) {
			log.info(" topic  queue1 message:"+message);
		}

		@RabbitListener(queues=MQconfig.TOPIC_QUEUE2)
		public void receiveTopic2(String message) {
			log.info(" topic  queue2 message:"+message);
		}

		@RabbitListener(queues=MQconfig.HEADER_QUEUE)
		public void receiveHeaderQueue(byte[] message) {
			log.info(" header  queue message:"+new String(message));
		}

}
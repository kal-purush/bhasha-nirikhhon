import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.QueueConnection;
import javax.jms.QueueReceiver;
import javax.jms.QueueSession;
import javax.jms.Session;

import com.ibm.mq.jms.MQQueueConnectionFactory;

public class MessageReceiver implements MessageListener {
	private QueueConnection conn;
	
	
	public MessageReceiver() throws JMSException {
		MQQueueConnectionFactory factory = new MQQueueConnectionFactory();
		factory.setHostName("localhost");
		factory.setPort(1414);
		factory.setQueueManager("QMA");
		conn = factory.createQueueConnection();
		conn.start();
		
	}
	
	public void start() throws JMSException, InterruptedException {
		QueueSession session = conn.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
		QueueReceiver receiver = session.createReceiver(session.createQueue("QUEUE1"));
		receiver.setMessageListener(this);
		
		Thread.sleep(30000);
	}
	
	public void close() throws JMSException {
		if(conn != null) {
			conn.close();
			conn = null;
		}
	}
	
	public static void main(String[] args) throws JMSException, InterruptedException {
		// TODO Auto-generated method stub
		MessageReceiver receiver = new MessageReceiver();
		receiver.start();
		receiver.close();
	}

	public void onMessage(Message msg) {
		// TODO Auto-generated method stub
		try {
			System.out.println("Message " + msg.getJMSMessageID() + " received.");
		} catch (JMSException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
package message_queue;

import java.util.LinkedList;
import java.util.Queue;

public class QueueStudy {
	public static void main(String[] args) {
		Queue<Message> messageQueue = new LinkedList<Message>();
		
		messageQueue.offer(new Message("sendMail", "이현규"));
		messageQueue.offer(new Message("sendSMS", "송다현"));
		messageQueue.offer(new Message("sendKakaotalk", "한민호"));
		
		while(!messageQueue.isEmpty()) {
			Message message = messageQueue.poll();
			switch(message.command) {
			case "sendMail":
				System.out.println(message.to + "님에게 메일을 보냅니다.");
				break;
			case "sendSMS":
				System.out.println(message.to + "님에게 SMS을 보냅니다.");
				break;
			case "sendKakaotalk":
				System.out.println(message.to + "님에게 Kakaotalk을 보냅니다.");
				break;
			}
		}
	}
}
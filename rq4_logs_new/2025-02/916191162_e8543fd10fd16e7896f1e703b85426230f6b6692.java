package QueueLinkedList;

public class Main {

	public static void main(String[] args) {

		QueueLinkedList qll = new QueueLinkedList();
		qll.enQueue(1);
		qll.enQueue(3);
		qll.enQueue(1);
		System.out.println(qll.deQueue());
		System.out.println(qll.deQueue());
	}

}
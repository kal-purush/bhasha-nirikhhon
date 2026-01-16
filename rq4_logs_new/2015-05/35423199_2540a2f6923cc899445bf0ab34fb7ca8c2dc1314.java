package oop02.syntax;

public class CardMain {

	public static void main(String[] args) {
		
		System.out.println("ī�� ���� : "+CardVO.height);
		System.out.println("ī�� �ʺ� : "+CardVO.wiath);
		
		// �ν��Ͻ� �����Ͽ� �Ʒ� ����� �������� ����϶�
		CardVO vo = new CardVO("heart",3);
		
		System.out.println("�÷��̾�� "+vo.kind+" "+vo.number+"�̰�");//heart,spade
		System.out.println("��ǻ�ʹ� "+vo.com+"�� ��ǻ�Ͱ� �̰���ϴ�.");
	}

}
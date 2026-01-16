package aio;

import java.util.Scanner;

import aio.client.Client;
import aio.server.Server;

/** 
* @Description: ���Է��� 
* @author hjd
* @date 2017��1��12�� ����3:55:49 
*  
*/
public class TestAIO {
	//����������
	public static void main(String[] args) throws Exception{
		//���з�����
		Server.start();
		//˯��100���룬����ͻ������ڷ���������ǰִ�д���
		Thread.sleep(100);
		//���пͻ���
		Client.start();
		System.out.println("������������Ϣ��");
        Scanner scanner = new Scanner(System.in);
        while(Client.sendMsg(scanner.nextLine()));
	}
}
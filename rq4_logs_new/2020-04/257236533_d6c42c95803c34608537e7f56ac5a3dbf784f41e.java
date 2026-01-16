package part06_string;

import java.util.Scanner;

// �ܾ��� ���� (���� 1152��)
// 1) ���� ��ҹ��ڿ� ���⸸���� �̷���� ���ڿ� �Է�
// 2) �ܾ��� ���� ��� (�ܾ�� ���� �� ���� ����)
public class Ex05_wordCount {

	static Scanner sc = new Scanner(System.in);

	public static void main(String[] args) {

		String str = ""; // ���� ��ҹ��ڿ� ���⸸���� �̷���� ���ڿ�
		int cnt = 1; // �ܾ��� ����

		System.out.print("���ڿ� �Է� -> ");
		// nextLine() : ������ �����Ͽ� ���ڿ� ����
		// next() : �������� �����Ͽ� ���ڿ� ����
		str = sc.nextLine();
		// �� ��, �� �� ���� ����
		str = str.trim();

		for (int i = 0; i < str.length(); i++) {
			if (str.charAt(i) == ' ') { // �ڸ� ���� ������ ���
				if (str.charAt(i - 1) == ' ') { // �� �� �ڸ� ���� ������ ���
					continue; // ���� �ڸ��� ���� �ѱ�
				}
				cnt++; // �ܾ��� ���� ����
			}
		} // end of for

		System.out.println(cnt);
	}
}
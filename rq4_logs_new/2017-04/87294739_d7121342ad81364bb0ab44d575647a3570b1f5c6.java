/*
 * ���ϰ� ���õ� �۾��� �����ִ� ���뼺�ִ� Ŭ������ ������¥
 * (ex . LodaMain���� Ȯ���� ��Ƴ���!)
 * 
 * 
 * �̸޼���� �ν��Ͻ� �޼��� ���� ��𼭵� �ټ��ְ� static 
 * */

package util.file;

public class FileUtil {
	//�Ѱܹ��� ��ο��� Ȯ���� ���ϱ�
	public static String getExt(String path){ //Ȯ���� �޾ƾ��ϴϱ� ��ȯ�� void -> String //�� �Ѱܹ����� �𸣴ϱ� �Ű����� String path
		//c:/aa/ddd/test....aa.jpg 
		int last=path.lastIndexOf(".");
		
		
		return path.substring(last+1,path.length());
		
		
	}
}
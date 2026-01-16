
/**
 * 
 *   	ni �ַ�������
 *   
 *    �ٶ��ڴ��������Ϊ�����ַ���ѹ���ռ䡣���Ǽ� aa = 2a , 2(ab) = abab
 *    ���ڸ������ַ������Ǽ� 2a(1a2b3(ab)) = aaabbababab
 *    ��д�㷨��ʵ�ֽ�ѹ���̡������ַ�����ֵ�����1~9,a~z,(,)
 *    ���ֵĴ�С���ᳬ��1000���ַ����ĳ��Ȳ��ᳬ��256��ע������ƥ�䡣
 *    
 */
public class NI20150428 {

	
	public static void decompress(String str) {
		
		if(str.length() == 0) return;
		int count = 0;
		
		for(int i=0;i<str.length();i++) {
			
			char ch = str.charAt(i);
		
			if(ch >= '1' && ch<= '9') {
				count = 10*count + ch - '0';
				continue;
			}  else if(ch >='a' && ch <= 'z') {
				if(count == 0) count = 1;
				while(count > 0){
					System.out.print(ch);
					count --;
				}
			}  else if(ch == '(') {
				if(count == 0) count = 1;
				while(count > 0) {
					decompress(str.substring(i+1));
					count --;
				}
				while(i<str.length()) {
					char temp = str.charAt(i);
					if(temp == ')') break;
					i++;
				}
			} 
		}
	}
	
	
	
	public static void main(String[] args) {
		decompress("a3d2(ab2(ce1(po)))");
	}

}
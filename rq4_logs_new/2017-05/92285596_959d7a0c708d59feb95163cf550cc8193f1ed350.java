
public class Dentaku {
	public static void main(String args[]){
	    int val1 = 0;  /* �ŏ��̐��� */
	    int val2 = 0;  /* ���̐��� */
	    String ope;    /* ���Z�q */
	    int kekka;     /* ���ʂ��i�[ */

	    if (args.length != 3){
	      errDisp("������3���͂��ĉ�����");
	    }

	    try{
	      val1 = Integer.parseInt(args[0]);
	      val2 = Integer.parseInt(args[2]);
	    }catch(NumberFormatException e){
	      errDisp("���l�łȂ��l�����͂���Ă��܂�");
	    }

	    ope = args[1];

	    String opeStr[] = {"kake", "waru", "tasu", "hiku"};
	    boolean errFlag = true;

	    for (int i = 0 ; i < 4 ; i++){
	      if (ope.equals(opeStr[i])){
	        errFlag = false;
	      }
	    }

	    if (errFlag == true){
	      errDisp("���Z�q�� kake waru tasu hiku ��4�ł�");
	    }

	    if (ope.equals("kake")){
	      kekka = val1 * val2;
	    }else if (ope.equals("tasu")){
	      kekka = val1 + val2;
	    }else if (ope.equals("hiku")){
	      kekka = val1 - val2;
	    }else{
	      if (val2 == 0){
	        errDisp("0 �Ŋ��낤�Ƃ��܂���");
	      }

	      kekka = val1 / val2;
	    }

	    System.out.println("���͂��ꂽ���� " + val1 + " " + ope + " " + val2 + " �ł�");
	    System.out.println("�v�Z���ʂ� " + kekka + " �ł�");
	  }

	  private static void errDisp(String errStr){
	    System.out.println("Usage : java dentaku ���l ���Z�q ���l");
	    System.out.println(errStr);
	    System.exit(0);  /* �v���O�������I������ */
	  }

}
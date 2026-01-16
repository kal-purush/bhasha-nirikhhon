package ocp01_syntax;

public class AverageA {
	String name;
	int kor;
	int eng;
	double avg;
	int tot; //합계
	
	public static void main(String[] args) {
		/*학생 객체생성 및 메모리 할당*/
		AverageA Hulk = new AverageA();
		AverageA Iron = new AverageA();
		AverageA Hawk = new AverageA();
		/*이름 저장*/
		Hulk.name = "헐클";
		Iron.name = "아이언";
		Hawk.name = "호크";
		
		/*정수 저장*/
		Hulk.kor = 100;
		Hulk.eng =80;
		Iron.kor = 50;
		Iron.eng = 90;
		Hawk.kor = 20;
		Hawk.eng = 40;
		
		/*총점 , 평균 계산*/
		Hulk.tot = Hulk.kor + Hulk.eng;
		Hulk.avg = Hulk.tot/2;
		Iron.tot = Iron.kor + Iron.eng;
		Iron.avg = Iron.tot/2;
		Hawk.tot = Hawk.kor + Hawk.eng;
		Hawk.avg = Hawk.tot/2;
		
		/*출력*/
		System.out.println("===" + Iron .name+ " 의 성적표===");
		System.out.println("국어 :" + Iron.kor);
		System.out.println("영어 :" + Iron.eng);
		System.out.println("합계 :" + Iron.tot);
		System.out.println("평균 :" + Iron.avg);
		System.out.println();
		
		System.out.println("===" + Hulk .name+ " 의 성적표===");
		System.out.println("국어 :" + Hulk.kor);
		System.out.println("영어 :" + Hulk.eng);
		System.out.println("합계 :" + Hulk.tot);
		System.out.println("평균 :" + Hulk.avg);
		System.out.println();
		
		System.out.println("===" + Hawk .name+ " 의 성적표===");
		System.out.println("국어 :" + Hawk.kor);
		System.out.println("영어 :" + Hawk.eng);
		System.out.println("합계 :" + Hawk.tot);
		System.out.println("평균 :" + Hawk.avg);
		System.out.println();
		
		
	}


}
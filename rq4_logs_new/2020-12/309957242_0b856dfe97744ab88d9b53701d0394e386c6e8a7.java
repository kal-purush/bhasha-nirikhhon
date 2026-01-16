package bokang.quiz;

import java.util.Random;
import java.util.Scanner;

public class BaseBallArrayGame {

	public static void main(String[] args) {
		
		Random random = new Random();
		Scanner scanner = new Scanner(System.in);
		
		//게임에서 사용할 변수
		int[] com = new int[3];		
		int[] user = new int[3];
		int randomCnt = 0;//난수생성 카운트
		int gameCount = 0;//게임 카운트
		int strikeCnt=0, ballCnt=0;//스크라이크,볼 카운트
		
		
		//게임전체반복	
		while(true) {
			//중복되지 않는 3개의 난수생성
			while(true) {				
				//난수 중복확인을 위한 변수
				boolean reTry = true;
				
				//몇번만에 난수가 생성되는지 확인
				randomCnt++;
				
				for (int i = 0; i<com.length ; i++) {
					com[i] = random.nextInt(100) % 9 + 1;
				}
	 
				//해당 방식은 변수를 사용할때의 로직
//				if(!(com[0]==com[1] || com[1]==com[2] || com[2]==com[0])) {				
//					//중복되지 않는 난수 생성에 성공하면 루프탈출
//					break;
//				}
				
				//비교를 위해 배열의 크기만큼 반복
				for(int i=0 ; i<com.length ; i++) {
					int j = i + 1;
					if(j==com.length) j = 0;
					if(com[i]==com[j]) {
						//중복되는 난수를 발견하면 false로변경
						reTry = false;
					}
				}
				
				//중복되는 난수가 없다면..
				if(reTry==true) break;
			}////while end
			
			//난수확인
			System.out.println(randomCnt+"회:"+
						com[0]+" "+com[1]+" "+com[2]);
			
			while(true) {
				//사용자로부터 3개의 정수를 입력받는다.
				System.out.println("3개의정수를 입력하세요(1~9)");
				System.out.println("스페이스로 구분하시고 마지막에"
						+ " Enter를 눌러주세요");
				
				for (int i = 0; i < user.length; i++) {
					user[i] = scanner.nextInt();					
				}
				
				//게임카운트 1회 증가
				gameCount++;
				
				//스크라이크 및 볼 판정
				for (int i = 0; i < com.length; i++) {
					for (int j = 0; j < user.length; j++) {
						//판단1 - 숫자가 일치하고
						if(com[i]==user[j]) {
							//판단2 - 자리가 일치하면
							if(i==j) {		
								//일치하면 스크라이크
								strikeCnt++;
							}
							else {
								//일치하지 않으면 볼
								ballCnt++;
							}
						}
					}
				}
								
				
				//게임종료판단
				if(strikeCnt==3) {
					System.out.println("3스트라이크 게임종료");
					System.out.println(gameCount+"번만에 맞추셨습니다.");
					break;//루프탈출
				}
				else {
					//하나도 못맞추는 경우
					if(strikeCnt==0 && ballCnt==0) {
						System.out.println("아웃입니다");
					}
					else {
						System.out.printf("%d스트라이크, %d볼\n",
								strikeCnt, ballCnt);
					}
					//스트라이크, 볼 카운트 초기화
					strikeCnt = 0;
					ballCnt = 0;
					//continue문은 필요없음.
				}
			}////while end
			
			
			System.out.println("한게임 더 하시겠습니까?(0:종료,1:재시작)");
			int restart = scanner.nextInt();
			if(restart==0) {
				//게임종료
				System.out.println("\n==게임이 종료되었습니다.==\n");
				//break;
				
				//실행되는 즉시 main함수가 종료된다.
				System.exit(0);
			}
			else if(restart==1){
				//게임을 재시작하기 위해 카운트변수 초기화
				strikeCnt = 0;
				ballCnt = 0;
				gameCount = 0;
				System.out.println("\n==게임을 재시작합니다.==\n");
			}
		}//게임 전체 반복 while end
	}
}
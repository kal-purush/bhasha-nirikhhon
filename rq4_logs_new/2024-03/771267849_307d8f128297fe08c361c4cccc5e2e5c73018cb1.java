package Accounting.Saving;
import java.util.ArrayList;
import java.util.Scanner;

import MiniProject.Members.Members;


public class SavingService {
	private SavingDao dao;
	public static String loginId;

	public SavingService() {
		dao = new SavingDao();
	}
	
	//Account_num(계좌번호),String id(계좌주),int balance(잔액),Date date(가입일),
	//Date expDate(만기일),double doublePercent(이자율)
	//적금 조회(전체 조회)
	public void printAll(Scanner sc) {
		System.out.println("적금 계좌 확인");
		ArrayList<Saving> list = dao.SelectAll(loginId);
		if(list.isEmpty()) {
			System.out.println("적금계좌가 존재하지 않습니다.");
		}else {
			for(Saving s:list) {
				System.out.println(s);
			}
		}
	}
	//계좌 번호 조회
	public void printAccount(Scanner sc) {
		System.out.println("계좌 확인");
		System.out.print("조회할 계좌 번호를 입력하세요:");
		int account_num = sc.nextInt();
		//계좌의 주인일 때도 조회.
		Saving s = dao.selectByNum(account_num);
		if(s.equals(null)|| !s.getId().equals(loginId)) {
			System.out.println("조회 할 수 없는 계좌입니다.");
			return;
		}
		//조회시 날짜가 음수이면 > exprie 확인후 0이면 이자 지급
		if(dao.getDate(account_num)<=0 && s.getExpiry()==0) {
			dao.updateExp(s);
		}
		System.out.println(s);
	}
	//입출금
	public void editBalance(Scanner sc) {
		System.out.print("계좌 번호를 입력하세요:");
		int account_num = sc.nextInt();
		//계좌의 주인일 때도 조회.
		Saving s = dao.selectByNum(account_num);
		if(s.equals(null)|| !s.getId().equals(loginId)) {
			System.out.println("접근할 수 없는 계좌입니다.");
			return;
		}
		if(dao.getDate(account_num)>0){
			System.out.println("만기 전 계좌입니다.");
			return;
		}
		System.out.print("출금 금액을 입력하세요:");
		int num = sc.nextInt();
		dao.update(s, (num*-1));
	}
	//계좌 번호 조회
	public void selectSaving(Scanner sc) {
		System.out.println("계좌 확인");
		System.out.print("조회할 계좌 번호를 입력하세요:");
		int account_num = sc.nextInt();
		//계좌의 주인일 때도 조회.
		Saving s = dao.selectByNum(account_num);
		if(s.equals(null)|| !s.getId().equals(loginId)) {
			System.out.println("조회 할 수 없는 계좌입니다.");
			return;
		}
		System.out.println(s);
	}
	//계좌 추가
	public void addSaving(Scanner sc) {
		System.out.println("신규 적금 가입");
		int exp = 0;
		double per;
		System.out.print("입금 금액을 입력하세요:");
		int balance = sc.nextInt();
		do{
			System.out.print("가입 개월을 선택하세요.(3개월 0.2%, 6개월 0.6%, 12개월 0.9%): ");
			exp = sc.nextInt();
		}
		while(!(exp==3||exp==6||exp==12));
		if(exp==3) {
			per = 0.2;
		}else if(exp==6){
			per = 0.6;
		}else {
			per = 0.9;
		};
		dao.insert(new Saving(0,loginId,balance,null,null,per,0) , exp);
	}

	//계좌 삭제
	public void delSaving(Scanner sc) {
		//계좌 번호 조회 후 금액이 0이면 삭제, 금액이 남으면 삭제 불가능
		System.out.println("계좌 삭제");
		System.out.print("삭제할 계좌 번호를 입력하세요.");
		int num = sc.nextInt();
		Saving s = dao.selectByNum(num);
		if(dao.getDate(num)>0) {
			System.out.println("만기 전 계좌입니다.");
		}
		if(s.getBalance()>0) {
			System.out.println("금액이 남아 있어 탈퇴가 불가능합니다.");
			System.out.println("금액을 출금 후 삭제가 가능합니다.");
			return;
		}
		dao.delete(num);
	}
}
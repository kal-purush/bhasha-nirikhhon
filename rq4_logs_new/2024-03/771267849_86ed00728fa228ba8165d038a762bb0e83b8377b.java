package Accounting;

import java.util.ArrayList;
import java.util.Scanner;

import MiniProject.Members.MembersService;

public class AccountService {
	private AccountDao dao;

	public AccountService() {
		dao = new AccountDao();
	}

	public void addAccount(String id) {
		System.out.println("===계좌추가===");
		dao.insert();
		System.out.println("===개설이 완료되었습니다===");
		System.out.println("===사용전 관리자의 승인이 필요하니다===");
	}

	public void getByNum(Scanner sc) {
		System.out.println("===계좌번호로 검색===");
		System.out.println("계좌번호:");
		String account_num = sc.next();
		Account1 account = dao.selectByNum(account_num);
		if (account == null) {
			System.out.println("없는 계좌번호");
		} else {
			if (MembersService.loginId.equals(account.getId())) {//
				System.out.println(account);
				System.out.println("1.입금하기 2.송금하기 3.계좌내역보기 4.계좌삭제하기");
				int x = sc.nextInt();
				switch (x) {
				case 1:
					deposit(sc, account_num);
					System.out.println(dao.selectByNum(account_num));
					break;
				case 2:
					withdraw(sc, account_num);
					System.out.println(dao.selectByNum(account_num));
					break;
				case 3:
					// 계좌내역보기
					break;
				case 4:
					delAccount(account_num);
					break;
				}
			} else {
				System.out.println("본인의 계좌가 아닙니다.");
			}
		}
	}

	public void getAll() {
		System.out.println("===전체 계좌===");
		ArrayList<Account1> list = dao.SelectAll();
		if (list.isEmpty()) {
			System.out.println("검색 결과가 없습니다");
		} else {
			for (Account1 a : list) {
				if (MembersService.auth == false) {// 은행원이 아니면
					if (a.getId().equals(MembersService.loginId)) {
						// 로그인한 아이디와 같은 계좌들만 print
						System.out.println(a);
					}
				} else {
					// 은행원이면 전부 프린트
					System.out.println(a);
				}
			}
		}
	}

	public void deposit(Scanner sc, String account_num) {
		System.out.println("==입금==");
		System.out.println("얼마를 입금하시겠습니까?");
		int money = sc.nextInt();
		while (money < 0) {
			System.out.println("다시 입력해 주십시오.");
			money = sc.nextInt();
		};
		dao.update(money,account_num);
		System.out.println("입금이 완료되었습니다.");
	}

	public void withdraw(Scanner sc, String account_num) {
		System.out.println("==출금==");
		System.out.println("얼마를 출금하시겠습니까?");
		int money = sc.nextInt();
		while(money<0) {
			System.out.println("다시 입력해 주십시오.");
			money = sc.nextInt();
		}
		if (dao.selectByNum(account_num).getBalance() < money) {
			System.out.println("계좌에 잔액이 부족합니다.");
		} else {
			dao.update(money*-1,account_num);
			System.out.println("출금이 완료되었습니다.");
		}
	}

	public void delAccount(String num) {
		System.out.println("===삭제===");
		if (dao.selectByNum(num).getBalance() == 0) {
			dao.delete(num);
			System.out.println("삭제가 완료되었습니다");
		} else {
			System.out.println("계좌에 잔액이 없어야 삭제할 수 있습니다.");
		}
	}
}
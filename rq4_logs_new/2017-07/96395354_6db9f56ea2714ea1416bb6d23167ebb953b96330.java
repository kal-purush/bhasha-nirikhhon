package com.hanbit.account.controller;

import java.util.ArrayList;
import java.util.List;

import javax.swing.JOptionPane;

import com.hanbit.account.domain.AccountBean;
import com.hanbit.account.service.AccountService;
import com.hanbit.account.serviceImpl.AccountServiceImpl;

public class AccountController {
public static void main(String[] args) {
	AccountBean account=null;
	List<AccountBean> accounts=null;
	String list="";
		AccountService service=new AccountServiceImpl();
		while(true){
			switch(JOptionPane.showInputDialog("1.계좌생성 2.모든계좌보기 3.계좌번호로 계좌조회 4.이름으로 계좌조회 5.비밀번호변경 6.입금 7.출금 8.계좌삭제")){
			case "1":
				account=new AccountBean();
				String arr[]=JOptionPane.showInputDialog("이름/비번/아이디/입금액/주민번호/계좌번호").split("/");
				account.setName(arr[0]);
				account.setPw(arr[1]);
				account.setId(arr[2]);
				account.setDeposit(arr[3]);
				account.setSsn(arr[4]);
				account.setAccountNumber(arr[5]);
				account.setRegdate(service.getTodayTime());
				JOptionPane.showMessageDialog(null, "계좌생성 완료 !");
				break;
			case "2": 
				List<AccountBean> AccountList=new ArrayList<>();
				AccountList=service.AccountList();
				AccountBean[] AllList=new AccountBean[AccountList.size()];
				int index=0;
				for(AccountBean temp : AccountList){
					AllList[index]=temp;
				}
				
				for(int i=0;i<AllList.length;i++){
					list+=AllList[i].toString();
				}
				JOptionPane.showMessageDialog(null, list);
				break;
			case "3": 
				account=service.findByAccount(JOptionPane.showInputDialog("계좌번호를 입력해주세요"));
				JOptionPane.showMessageDialog(null, account.toString());
				break;
			case "4":
				accounts=service.findByName(JOptionPane.showInputDialog("이름을 입력해주세요"));
				AccountBean[] SomeList=new AccountBean[accounts.size()];
				int index2=0;
				for(AccountBean temp : SomeList){
					SomeList[index2]=temp;
				}
				
				for(int i=0;i<SomeList.length;i++){
					list+=SomeList[i].toString();
				}
				JOptionPane.showMessageDialog(null, list);
				break;
			case "5": 
				account=new AccountBean();
				account.setId(JOptionPane.showInputDialog("아이디를 입력해주세요"));
				account.setPw(JOptionPane.showInputDialog("새로운 비밀번호를 입력해주세요"));
				service.updatePw(account);
				JOptionPane.showMessageDialog(null, "변경완료");
				break;
			case "6": 
				service.deposit(JOptionPane.showInputDialog("입금액을 입력해주세요"));
				JOptionPane.showMessageDialog(null, "입금완료");
				break;
			case "7":
				service.withdraw(JOptionPane.showInputDialog("출금액을 입력해주세요"));
				break;
			case "8": 
				
				break;
				
				
			}
		}
	}
}
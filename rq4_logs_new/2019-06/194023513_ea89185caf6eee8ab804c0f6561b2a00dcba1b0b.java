package com.internousdev.blue.action;

import java.util.Map;

import org.apache.struts2.interceptor.SessionAware;

import com.opensymphony.xwork2.ActionSupport;

public class CreateDestinationAction extends ActionSupport implements SessionAware{
	private int backFlg;
	private Map<String,Object> session;

	public String execute(){
		if(!session.containsKey("tempUserId") && !session.containsKey("userId")){
			return "sessionTimeout";
		}

		if(backFlg !=1){//戻るボタンで戻ってきたので無ければsessionの中身を消しておく。
			session.remove("familyName");
			session.remove("firstName");
			session.remove("familyNameKana");
			session.remove("firstNameKana");
			session.remove("email");
			session.remove("telNumber");
			session.remove("userAddress");
		}
		return SUCCESS;
	}

	public int getBackFlg(){
		return backFlg;
	}
	public void setBackFlg(int backFlg){
		this.backFlg = backFlg;
	}

	public Map<String,Object> getSession(){
		return session;
	}
	public void setSession(Map<String, Object> session){
		this.session = session;
	}
}
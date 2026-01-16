package com.bang;

import com.bang.im.rest.*;

public class RestDebug {
	public static void main(String[] args) {
		//SMS sms = new SMS();
		//sms.execute();
		SubAccounts su = new SubAccounts(1,5);
		//su.execute();
		/*su.nextPage();
		su.nextPage();
		su.nextPage();
		su.nextPage();*/
		su.nextPage();
		//su.closeSubAccount("closeSubAccount");
		su.querySubAccountByName("IOS-VoIP����DEMO");
		su.accountInfo();
	}
}
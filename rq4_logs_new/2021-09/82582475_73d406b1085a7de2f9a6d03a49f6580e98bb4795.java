package com.foc.web.microservice.entity;

import com.foc.Globals;
import com.foc.admin.ActiveUserList;
import com.foc.admin.FocUser;
import com.foc.admin.FocUserDesc;
import com.foc.list.FocList;
import com.foc.web.microservice.FocMicroServlet.SessionAndApplication;

public class AuthTokenHandler_FocDefault implements IAuthTokenHandler {

	public AuthTokenHandler_FocDefault() {
	}
		
	@Override
	public AuthTokenHandlerResult decodeToken(SessionAndApplication session, String token) {
		AuthTokenHandlerResult result = null;

		FocSimpleTokenAuth auth = new FocSimpleTokenAuth();
		String username = auth.verifyToken(token);

		if(username != null){
			FocList list = FocUserDesc.getInstance().getFocList();
			if(list != null){
				list.loadIfNotLoadedFromDB();
				FocUser user = (FocUser) list.searchByPropertyStringValue(FocUserDesc.FLD_NAME, username, false);
				// Reload once if we don't find. This is in case a new user was
				// created
				if(user == null){
					Globals.logString(" = Username: " + username + " not found reloading user list");
					list.reloadFromDB();
					user = (FocUser) list.searchByPropertyStringValue(FocUserDesc.FLD_NAME, username, false);
				}

				if(user != null && !user.isSuspended()){
					result = new AuthTokenHandlerResult();
					result.setUsername(username);
					
					session.getWebSession().setFocUser(user);
					ActiveUserList.getInstance().serviceSide_updateHeartbeat(user);
					Globals.logString(" = Session opened for username: " + username);
				}else{
					Globals.logString(" = Username: " + username + " not found, logout()");
				}
			}else{
				Globals.logString(" = FocUser list null");
			}
		}
		
		return result;
	}
	
}
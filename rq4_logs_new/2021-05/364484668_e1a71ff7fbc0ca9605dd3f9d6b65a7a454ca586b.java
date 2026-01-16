package in.casekartin.service;

import in.casekartin.exception.ServiceException;
import in.casekartin.exception.StringException;
import in.casekartin.exception.ValidationException;
import in.casekartin.util.LoginRegisterUtil;
import in.casekartin.util.StringNumberUtil;
import in.casekartin.validator.LoginValidator;

public class LoginService {
	private LoginService(){
		//default constructor
	}
	/**
	 * 
	 * @param userName
	 * @param password
	 * @return
	 * @throws ServiceException 
	 */
	public static boolean isloginSuccess(String userName, String password) throws ServiceException {
		try {
			StringNumberUtil.stringUtil(userName);
			LoginRegisterUtil.isUserNameCharAllowed(userName);
			LoginRegisterUtil.isPasswordCharAllowed(password);
			LoginValidator.isLoginVerified(userName,password);
		} catch (StringException | ValidationException e) {
			throw new ServiceException(e.getMessage(),e);
		}
		return false;
	}

}
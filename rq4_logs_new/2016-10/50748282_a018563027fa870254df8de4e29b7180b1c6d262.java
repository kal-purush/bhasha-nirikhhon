package business;

import db.ConnectionPool;

public class ConnectionPoolGGS extends ConnectionPool {

	@Override
	protected String getPassword() {
		return DBUtil.PWD;
	}

	@Override
	protected String getUser() {
		return DBUtil.USR;
	}

	@Override
	protected String getDriver() {
		return DBUtil.DRIVERCLASSNAME;
	}

	@Override
	protected String getDbUrl() {
		return DBUtil.URL;
	}

	@Override
	protected int getNumeroConnessioniDisponibili() {
		return 5;
	}

}
package core.jdbc;

import javax.annotation.Resource;
import javax.sql.DataSource;

import org.springframework.jdbc.core.support.JdbcDaoSupport;

public class AbstractJdbcDaoSupport extends JdbcDaoSupport {
	@Resource(name="dataSource")
	public void setDS(DataSource dataSource) {
		super.setDataSource(dataSource);
	}
}
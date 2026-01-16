package hal_shop.util.dao.sales;

import java.sql.ResultSet;
import java.sql.SQLException;

import hal_shop.util.ResultSetMapping;

public class SaleMapping extends ResultSetMapping<SaleDTO> {

	protected String primaryKey = "sales_no";
	protected String table = "sales";
	
	@Override
	public SaleDTO setMapping(ResultSet rs)throws SQLException{
		SaleDTO sd = new SaleDTO();
		sd.setNo(rs.getInt("sales_no"));
		sd.setCustomerNo(rs.getInt("customer_no"));
		sd.setDate(rs.getString("sales_date"));
		return sd;
	}

}
package com.gree.dataAccessObject;

import java.sql.Connection;
import java.sql.PreparedStatement;

import com.gree.model.Supplier;


/**
 * ͼ��DAO��
 * @author Administrator
 *
 */
public class SupplierDAO {
	
	/**
	 * ��������
	 * @param con
	 * @param Supplier
	 * @return
	 * @throws Exception
	 */
	public int add(Connection con,Supplier supplier) throws Exception{
		String sql = "insert into t_supplier values(null,?,?,?,?,?)";
		PreparedStatement pstmt = con.prepareStatement(sql);
		pstmt.setString(1, supplier.getSupplierCode());
		pstmt.setString(2, supplier.getSupplierName());
		pstmt.setString(3, supplier.getSuppliersupplierManager());
		pstmt.setString(4, supplier.getSupplierTel());
		pstmt.setString(5, supplier.getSupplierCity());
		return pstmt.executeUpdate();
	}
	
	/**
	 * ͼ����Ϣ��ѯ
	 * @param con
	 * @param Supplier
	 * @return
	 * @throws Exception 
	 */
	/*public ResultSet list(Connection con,Supplier Supplier) throws Exception{
		StringBuffer sb = new StringBuffer("select * from t_Supplier b,t_SupplierType bt where b.SupplierTypeId=bt.id");
		if(StringUtility.isNotEmpty(Supplier.getSupplierName())){
			sb.append(" and b.SupplierName like '%" +Supplier.getSupplierName() + "%'");//ƴ�Ӳ�ѯ����
		}
		if(StringUtility.isNotEmpty(Supplier.getAuthor())){
			sb.append(" and b.author like '%" + Supplier.getAuthor()+"%'");
		}
		if(Supplier.getSupplierTypeId()!=null && Supplier.getSupplierTypeId()!=-1){
			sb.append(" and b.SupplierTypeId=" + Supplier.getSupplierTypeId());
		}
		PreparedStatement pstmt = con.prepareStatement(sb.toString());
		return pstmt.executeQuery();
	}*/
	
	/**
	 * ͼ����Ϣɾ��
	 * @param con
	 * @param id
	 * @return
	 * @throws Exception
	 */
	public int delete(Connection con,String id) throws Exception {
		String sql = "delete from t_supplier where id=?";
		PreparedStatement pstmt = con.prepareStatement(sql);
		pstmt.setString(1, id);
		return pstmt.executeUpdate();
	}
	
	/**
	 * ͼ����Ϣ�޸�
	 * @param con
	 * @param Supplier
	 * @return
	 * @throws Exception 
	 */
	/*public int update(Connection con,Supplier Supplier) throws Exception{
		String sql = "update t_Supplier set SupplierName=?,author=?,sex=?,price=?,SupplierDesc=?,SupplierTypeId=? where id=?";
		PreparedStatement pstmt = con.prepareStatement(sql);
		pstmt.setString(1, Supplier.getSupplierName());
		pstmt.setString(2, Supplier.getAuthor());
		pstmt.setString(3, Supplier.getSex());
		pstmt.setFloat(4, Supplier.getPrice());
		pstmt.setString(5, Supplier.getSupplierDesc());
		pstmt.setInt(6, Supplier.getSupplierTypeId());
		pstmt.setInt(7, Supplier.getId());
		return pstmt.executeUpdate();
	}*/
	
	/**
	 * ָ��ͼ��������Ƿ����ͼ�飿
	 * @param con
	 * @param SupplierTypeId
	 * @return
	 * @throws Exception 
	 */
	/*public boolean existSupplierBySupplierId(Connection con,String SupplierId) throws Exception{
		String sql = "select * from t_Supplier where SupplierId=?";
		PreparedStatement pstmt = con.prepareStatement(sql);
		pstmt.setString(1, SupplierId);
		ResultSet rs=pstmt.executeQuery();
		return rs.next();
	}*/
	
}
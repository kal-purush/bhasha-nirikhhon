package com.free.module.common.dao;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.sql.DataSource;

public class CmAttachDao {
    private InitialContext ic = null;
    private Context ec = null;
    private DataSource ds = null;
    private Statement stmt = null;
    protected Connection conn = null;

    public CmAttachDao() throws Exception {
    }

    public int insertAttach() throws Exception {
        int cnt = 0;
        try {
            ic = new InitialContext();
            ds = (DataSource) ic.lookup("java:/comp/env/jdbc/CUBRIDDS");
            //ds = (DataSource) ec.lookup("jdbc/CUBRIDDS");

            conn = ds.getConnection();
            conn.setAutoCommit(false);
            
            // stmt = conn.createStatement();
            // String sql = "SELECT 1 name, 2 age, 3 old";
            // rs = stmt.executeQuery(sql);

            ResultSet rs = null;

            stmt = conn.createStatement();
            String sql = "insert into bbs_attach values (bbs_attach_seq.next_value, 'title', 'note', 'attach_path', 'attach_file_name', 'attach_real_name', 'input_user', to_char(sysdate, 'yyyymmdd'), TO_CHAR(SYSTIMESTAMP, 'hh24miss'), 'update_user', to_char(sysdate, 'yyyymmdd'), TO_CHAR(SYSTIMESTAMP, 'hh24miss'));";
            cnt = stmt.executeUpdate(sql);

            conn.commit();
            
            System.out.println(">>>>>>>>>>>>>>>>>>> : " + cnt);
        } catch (Exception e) {
            conn.rollback();
            throw e;
        } finally {
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException ex) {
                }
            }
            
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                }
            }
        }
        return cnt;
    }
}
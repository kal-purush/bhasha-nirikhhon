package daos.registry;

import entities.Registry;
import entities.Role;
import util.ConnectUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class RegistryDaoPostgres implements RegistryDao {

    @Override
    public boolean getPersonByLogin(String login, String pass) {
            Registry person2 = new Registry(0,"none", "none", Role.UNCHECKED, "none","none");
            boolean accurate = false;

        try(Connection conn = ConnectUtil.getConnection()){

            String sql = "select from registry where login = ?";
            PreparedStatement ps =conn.prepareStatement(sql);
            ps.setString(1, login);
            ResultSet rs = ps.executeQuery();

            if(rs.next()) {
                person2.setRegistryId(rs.getInt("registry_id"));
                person2.setFirstName(rs.getString("first_name"));
                person2.setLastName(rs.getString("last_name"));
                person2.setAccessRole(rs.getString("access_role"));
                person2.setLogin(rs.getString("login"));
                person2.setLogPass(rs.getString("log_pass"));

                if (person2.getLogPass() == pass){
                    accurate = true;
                }
            }

        }catch(SQLException e) {
            e.printStackTrace();
        }



        return accurate;
    }
}
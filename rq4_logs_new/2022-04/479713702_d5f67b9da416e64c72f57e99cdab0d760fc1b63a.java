
package dao;
import entity.About;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class AboutDao extends DBConnection{
    About findById(int id){
        About c = null;
        try {
            Statement st = this.getConnection().createStatement();
            String query = "select * from about where id="+id;
            
            ResultSet rs = st.executeQuery(query);
            while(rs.next()){
                c = new About(rs.getInt("id"), rs.getNString("detail"),rs.getString("image"),rs.getBoolean("status"));
            }
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return c;
    }
        
    public void create(About c){
        try {
            Statement st = this.getConnection().createStatement();
            String query = "insert into about(detail,image,status) values ('"+c.getDetail()+"','"+c.getImage()+"','"+c.getStatus()+"')";
            st.executeUpdate(query);
        
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public void delete(About c){
        try {
            Statement st = this.getConnection().createStatement();
            String query = "delete from about where id="+c.getId();
            st.executeUpdate(query);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public void update(About c){
        try {
            Statement st = this.getConnection().createStatement();
            String query = "update about set detail='"+c.getDetail()+"' where image="+c.getImage()+"' where status="+c.getStatus()+"' where id="+c.getId();
            st.executeUpdate(query);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public List<About> getList(){
        List<About> list = new ArrayList<>();
        try {
            Statement st = this.getConnection().createStatement();
            String query = "select * from about";
            
            ResultSet rs = st.executeQuery(query);
            while(rs.next()){
                list.add(new About(rs.getInt("id"), rs.getNString("detail"),rs.getString("image"),rs.getBoolean("status")));
            }
            
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return list;
    }
}
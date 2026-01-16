package dao;


import dao.DBConnection;
import entity.Category;
import java.sql.Statement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author CASPER
 */
public class CategoryDao extends DBConnection{
    public void create(Category c){
        try{
            Statement st=this.getConnection().createStatement();
            String query="insert into categorys (name) values('"+c.getName()+"')";
            st.executeQuery(query);
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
    public void update(Category c){
        try{
            Statement st=this.getConnection().createStatement();
            String query="update categorys set id='"+c.getId()+"', name='"+c.getName()+"', description='"+c.getDescription()+"' where id="+c.getId();
            st.executeQuery(query);
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
    public void delete(Category c){
        try{
            Statement st=this.getConnection().createStatement();
            String query="delete from categorys where id="+c.getId();
            st.executeQuery(query);
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
    }
    public List<Category> getList(){
        List<Category> list=new ArrayList<>();
        try{
            Statement st=this.getConnection().createStatement();
            String query="select * from categorys";
            ResultSet rs=st.executeQuery(query);
            while(rs.next()){
                list.add(new Category(rs.getInt("id"),rs.getString("name"),rs.getString("description"),rs.getBoolean("status")));
            }
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
        return list;
    }
    public Category findById(int id){
        Category c=null;
        try{
            Statement st=this.getConnection().createStatement();
            String query="select * from categorys where id="+id;
            ResultSet rs=st.executeQuery(query);
            while(rs.next()){
                c=new Category(rs.getInt("id"),rs.getString("name"),rs.getString("description"),rs.getBoolean("status"));
            }
        }
        catch(Exception e){
            System.out.println(e.getMessage());
        }
        return c;
    }
}

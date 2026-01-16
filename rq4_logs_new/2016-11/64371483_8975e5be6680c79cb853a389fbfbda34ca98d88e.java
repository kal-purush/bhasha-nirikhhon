import java.sql.*;

public class DrillConnection {
    public static void main(String[] args) {
        try {
            Class.forName("org.apache.drill.jdbc.Driver");
            Connection connection = DriverManager.getConnection("jdbc:drill:zk=192.168.10.239:2181/drill/mfm");
            Statement st = connection.createStatement();
            ResultSet rs = st.executeQuery("select * from  mongo.mfm.sys_dict");
            ResultSetMetaData rsmd = rs.getMetaData();
//            System.out.println("ResultSetMetadata.getColumnDisplaySize(1) :"+rsmd.getColumnDisplaySize(1));
            while (rs.next()) {
                System.out.println(rs.getString("name"));
           }
        } catch (Exception e) {
            System.out.println("驱动加载错误");
            e.printStackTrace();//打印出错详细信息
        }
    }
}
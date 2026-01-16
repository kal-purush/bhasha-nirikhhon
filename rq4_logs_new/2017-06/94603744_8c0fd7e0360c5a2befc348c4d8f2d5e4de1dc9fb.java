package conn;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLData;
import java.sql.SQLException;
import java.sql.SQLInput;
import java.sql.SQLOutput;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.Map;

class Employee implements SQLData {
  public BigDecimal SSN;

  public String FirstName;

  public String LastName;

  public BigDecimal Salary;

  private String sqlUdt;

  public void writeSQL(SQLOutput stream) throws SQLException {
    stream.writeBigDecimal(SSN);
    stream.writeString(FirstName);
    stream.writeString(LastName);
    stream.writeBigDecimal(Salary);
  }

  public String getSQLTypeName() throws SQLException {
    return sqlUdt;
  }

  public void readSQL(SQLInput stream, String typeName) throws SQLException {
    sqlUdt = typeName;
    SSN = stream.readBigDecimal();
    FirstName = stream.readString();
    LastName = stream.readString();
    Salary = stream.readBigDecimal();
  }

  public String calcMonthlySalary() {
    double monthlySalary = Salary.doubleValue() / 12;
    NumberFormat nf = NumberFormat.getCurrencyInstance();
    String str = nf.format(monthlySalary);
    return str;
  }

}

public class Main {

  public static void main(String[] args) throws Exception {
    Class.forName("com.mysql.jdbc.Driver");

    Connection conn = DriverManager.getConnection("jdbc:mysql://localhost/double?" +
			"user=root&password=root");

    Statement stmt = conn.createStatement();

//    Map map = conn.getTypeMap();
//    map.put("EMP_DATA", Class.forName("Employee"));
//    conn.setTypeMap(map);

    ResultSet rs = stmt.executeQuery("SELECT * from Emp");

    Employee employee;

    while (rs.next()) {
//      int empId = rs.getInt("EmpId");
      employee = (Employee) rs.getObject("obj");

//      System.out.print("Employee Id: " + empId + ", SSN: " + employee.SSN);
//      System.out.print(", Name: " + employee.FirstName + " " + employee.LastName);
//      System.out.println(", Yearly Salary: $" + employee.Salary + " Monthly Salary: "
//          + employee.calcMonthlySalary());
//    }
//    conn.close();
  }
    }
  }


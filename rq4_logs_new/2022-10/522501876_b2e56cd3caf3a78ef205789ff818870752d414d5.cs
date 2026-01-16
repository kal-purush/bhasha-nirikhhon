using System.Collections.Generic;
using System.Data.SqlClient;

namespace AutoAuctionProjekt.Classes.Data.Adapters; 

public static class UserAdapter {

    public static IEnumerable<PrivateUser> GetPrivateUsers(SqlConnection connection) =>
        Database.GetAll(connection, PrivateUsersQuery, PrivateUserFromReader);
    
    public static IEnumerable<CorporateUser> GetCorporateUsers(SqlConnection connection) =>
        Database.GetAll(connection, PrivateUsersQuery, CorporateUserFromReader);

    #region SQL Queries

    private const string UsersCommon = @"Users.ID, Users.UserName, Users.ZipCode, Users.Balance";
    
    private const string PrivateUsersQuery = $@" 
SELECT {UsersCommon}, PrivateUsers.CprNumber 
FROM Users INNER JOIN PrivateUsers ON Users.ID = PrivateUsers.ID";
    
    private const string CorporateUsersQuery = $@"
SELECT {UsersCommon}, CorporateUsers.CvrNumber, CorporateUsers.CreditScore
FROM Users INNER JOIN CorporateUsers ON Users.ID = CorporateUsers.ID";
    #endregion


    public static PrivateUser PrivateUserFromReader(SqlDataReader reader) {
        return new PrivateUser(
            (string) reader["UserName"],
            (uint) reader["ZipCode"],
            (decimal) reader["Balance"],
            (string) reader["CprNumber"]) { ID = (uint) reader["ID"] };
    }
    
    public static CorporateUser CorporateUserFromReader(SqlDataReader reader) {
        return new CorporateUser(
            (string) reader["UserName"],
            (uint) reader["ZipCode"],
            (decimal) reader["Balance"],
            (string) reader["CvrNumber"],
            (int) reader["CreditScore"]) { ID = (uint) reader["ID"] };
    }
}
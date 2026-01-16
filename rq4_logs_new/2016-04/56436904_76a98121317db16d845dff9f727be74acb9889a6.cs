using System;
using System.Data.SqlClient;
namespace AMTS
{
    public class User
    {
        private bool hasTeam;
        private string teamName;
        private string email;
        private bool pendingTeamRequest;

        public User(SqlConnection connection, string email)
        {
            hasTeam = false;
            pendingTeamRequest = false;
            this.email = email;
            teamName = "";
            SqlCommand sqlcomm = new SqlCommand("SELECT Druzyna AS TEAM, PESEL AS PSL FROM UZYTKOWNICY WHERE Mail=" + "'" + email + "'", connection);
            string pesel;
            SqlDataReader r = sqlcomm.ExecuteReader();
            r.Read();
            Object team = r["TEAM"];
            if (DBNull.Value != team)
            {
                teamName = team.ToString();
                hasTeam = true;
            }
            else
            {
                pesel = r["PSL"].ToString();
                r.Close();
                SqlCommand sqlcomm2 = new SqlCommand("SELECT Nazwa AS TEAMNAME FROM DRUZYNY WHERE Kapitan = '" + pesel + "'", connection);
                SqlDataReader r2 = sqlcomm2.ExecuteReader();
                if (r2.Read())
                {
                    pendingTeamRequest = true;
                    teamName = r2["TEAMNAME"].ToString();
                }
                r2.Close();
            }
            r.Close();
        }

        public string getTeamName()
        {
            return teamName;
        }

        public string getEmail()
        {
            return email;
        }

        public bool getHasTeam()
        {
            return hasTeam;
        }

        public bool getPending()
        {
            return pendingTeamRequest;
        }

    }
}
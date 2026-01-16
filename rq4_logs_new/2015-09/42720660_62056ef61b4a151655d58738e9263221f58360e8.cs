using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace ApteanEdgeBank
{
    class AccountActivityLedger
    {
       // int activityID;
       public int accountID;
        public string transactionType;
        public double transactionAmount;
        public DateTime transactionDate;

        public AccountActivityLedger()
        {
           
        }

        public bool CheckAccountExistence(int accID)
        {
            UserDAO dao = new UserDAO();
            DataTable accTable = new DataTable();
            string myQuery = "select * from Account where AccountID=" + accID;
            string connectionstring = "Data Source=WS003LT1553PRD;Initial Catalog=ApteanEdgeBank;User=sa;Password=abc-123";
            accTable = dao.GetData(myQuery, connectionstring);
            if (accTable.Rows.Count > 0)
            {
                return true;
            }
            return false;
        }

        public void AddAccountActivity(int accID, string transType, double amount, DateTime transDate)
        {
            UserDAO dao = new UserDAO();
            string myQuery = "insert into AccountActivityLedger values(" + accID + "," + "'" + transDate + "'" + "," + "'" + transType + "'" + "," + amount + ")";
            string connectionstring = "Data Source=WS003LT1553PRD;Initial Catalog=ApteanEdgeBank;User=sa;Password=abc-123";
           // if (CheckAccountExistence(accID))
            //{
                dao.InsertData(myQuery, connectionstring);
            //}
           /* else
            {
                MessageBox.Show("Account Does not exist!");
            }*/
        }

        public DataTable ReadAccountActivity(int accId)
        {
            UserDAO dao = new UserDAO();
            string myQuery = "Select * from AccountActivityLedger where AccountID="+accId;
            DataTable activityTable = new DataTable();
            string connectionstring = "Data Source=WS003LT1553PRD;Initial Catalog=ApteanEdgeBank;User=sa;Password=abc-123";
            activityTable = dao.GetData(myQuery, connectionstring);
            return activityTable;
        }

        public double CalculateAccountBalance(int accId)
        {
            UserDAO dao = new UserDAO();
            string connectionstring = "Data Source=WS003LT1553PRD;Initial Catalog=ApteanEdgeBank;User=sa;Password=abc-123";
            //string myQuery = "Select * from AccountActivityLedger where AccountID=" + accId;
            string getCurrentBalance = "Select * from Account where AccountID=" + accId;
            double currentBalance=0.0;
            DataTable dt = new DataTable();
            dt=dao.GetData(getCurrentBalance, connectionstring);
            currentBalance = Convert.ToDouble(dt.Rows[0]["AccountBalance"]);

            /*double sum=currentBalance;
            DataTable activityLog = new DataTable();

            double accountBalance = 0.0;*/
            if (CheckAccountExistence(accId))
            {
               /* //sumDeposits = currentBalance;
                activityLog = dao.GetData(myQuery, connectionstring);
                foreach (DataRow row in activityLog.Rows)
                {
                    if (Convert.ToString(row["Activitycode"]) == "Deposit")
                    {
                        sum=sum+Convert.ToDouble(row["Amount"]);
                    }
                    else
                    {
                        sum=sum-Convert.ToDouble(row["Amount"]);
                    }
                }

                accountBalance = sum;
                string updateNewBalance = "Update Account set AccountBalance=" + accountBalance + " where AccountID=" + accId;
                dao.UpdateData(updateNewBalance, connectionstring);
                return accountBalance;*/
            return currentBalance;
            }
            else
            {
                MessageBox.Show("Account Does not exist!");
            }

            return 0;
        }
    }
}
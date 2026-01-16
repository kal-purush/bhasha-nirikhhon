using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BizWorks
{
    public class Transaction
    {
        private int transactionID;
        private bool isCredit;       // if false, then debit company account. If true then credit company account transactionAmount $
        private int CustomerID;
        private float transactionAmount; // USD
        private string category;
        private DateTime transactionDate;
        private int assetID;
        private string notes;
        
        private static List<Transaction> TransactionList = new List<Transaction>();
     

        //constructor
        public Transaction(bool isCredit, int customerID, float transactionAmount, string category,
        DateTime transactionDate, int assetID, string notes)
        {
            isCredit = isCredit;
            customerID = customerID;
            transactionAmount = transactionAmount;
            category = category;
            date = transactionDate;
            assetID = assetID;      // assetID == 1 is for cash, everything else is a material item eg. inventory
            notes = notes;
            transactionID = TransactionList.size();
            TransactionList.Add(new Transaction(isCredit, customerID, transactionAmount, category, date, assetId,notes));
            Asset.update(assetID, isCredit, transactionAmount);
        }

        public int transactionID
        {
            get { return transactionID; }
            set { transactionID = value; }
        }
        public bool isCredit
        {
            get { return isCredit; }
            set {isCredit = value; }
        }
        public int customerID
        {
            get { return customerID; }
            set { customerID = value; }
        }
        public string category
        {
            get { return category; }
            set { category = value; }
        }
     
        public DateTime date
        {
            get { return date; }
            set { date = value; }
        }
        
        public int assetID
        {
            get {return assetID;}
            set {assetID = value;}
        }
        
        public float transactionAmount
        {
            get { return transactionAmount; }
            set { tansactionAmount = value; }
        }
        
        public string notes
        {
            get { return notes; }
            set {notes = value; }
        }
        
    }
}
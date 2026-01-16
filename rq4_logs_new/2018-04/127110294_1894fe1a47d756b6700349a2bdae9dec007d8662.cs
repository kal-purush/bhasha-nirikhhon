using System;
using System.ComponentModel.DataAnnotations.Schema;
using System.ComponentModel.DataAnnotations;

namespace DataAccess.Entity.Entities
{
    class BankDeposit : BaseEntity
    {
        [Key]
        public int BankDepositID { get; set; }
        [Required]
        public int StoreID { get; set; }
        [Required]
        [Index]
        public DateTime TransactionTime { get; set; }
        [Required]
        public int BankAccountID { get; set; }
        [Required]
        public int TenderTypeID { get; set; }
        [DataType(DataType.Currency)]
        public float CashOpening { get; set; }
        [DataType(DataType.Currency)]
        public float CashPickups { get; set; }
        [DataType(DataType.Currency)]
        public float ReconciledEarnings { get; set; }
        [DataType(DataType.Currency)]
        public float BOExpenses { get; set; }
        [DataType(DataType.Currency)]
        public float CashDeclared { get; set; }
        [DataType(DataType.Currency)]
        public float CashDepositAmount { get; set; }
        [DataType(DataType.Currency)]
        public float CashClosing { get; set; }
    }
}
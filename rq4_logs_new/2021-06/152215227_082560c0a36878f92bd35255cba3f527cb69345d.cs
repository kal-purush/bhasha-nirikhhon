using Com.Danliris.Service.Finance.Accounting.Lib.Models.VBRequestDocument;
using Com.Danliris.Service.Finance.Accounting.Lib.Utilities.BaseClass;
using Com.Danliris.Service.Finance.Accounting.Lib.ViewModels.NewIntegrationViewModel;
using System;
using System.Collections.Generic;
using System.Text;

namespace Com.Danliris.Service.Finance.Accounting.Lib.BusinessLogic.VBRequestDocument
{
    public class ApprovalVBAutoJournalDto
    {
        //public string DocumentNo { get;  set; }
        //public DateTimeOffset Date { get;  set; }
        //public DateTimeOffset RealizationEstimationDate { get;  set; }
        //public int CurrencyId { get;  set; }
        //public string CurrencyCode { get;  set; }
        //public string CurrencySymbol { get;  set; }
        //public double CurrencyRate { get;  set; }
        //public string CurrencyDescription { get;  set; }
        //public string Purpose { get;  set; }
        //public decimal Amount { get;  set; }
        //public bool IsPosted { get;  set; }
        //public VBType Type { get;  set; }
        //public int SuppliantUnitId { get;  set; }
        //public string SuppliantUnitCode { get;  set; }
        //public string SuppliantUnitName { get;  set; }
        //public int SuppliantDivisionId { get;  set; }
        //public string SuppliantDivisionCode { get;  set; }
        //public string SuppliantDivisionName { get;  set; }
        //public int Index { get;  set; }
        //public bool IsRealized { get;  set; }
        //public ApprovalStatus ApprovalStatus { get;  set; }
        //public string CancellationReason { get;  set; }
        //public DateTimeOffset? ApprovalDate { get;  set; }
        //public string ApprovedBy { get;  set; }
        //public DateTimeOffset? CancellationDate { get;  set; }
        //public string CanceledBy { get;  set; }
        //public bool IsCompleted { get;  set; }
        //public DateTimeOffset? CompletedDate { get;  set; }
        //public string CompletedBy { get;  set; }
        //public bool IsInklaring { get;  set; }
        //public string NoBL { get;  set; }
        //public string NoPO { get;  set; }
        //public string TypePurchasing { get;  set; }
        public VBRequestDocumentModel VbRequestDocument { get; set; }
        public AccountBankViewModel Bank { get; set; }
    }
}
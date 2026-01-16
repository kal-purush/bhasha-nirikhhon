namespace KYC.DataBase.Models;

public class User
{
    public string Guid { get; set; }
    public string Status { get; set; }
    public string ClientId { get; set; }
    public string RecordId { get; set; }
    public string RefId { get; set; }
    public int SubmitCount { get; set; }
    public string BlockPassID { get; set; }
    public bool IsArchived { get; set; }
    public string InreviewDate { get; set; }
    public string WaitingDate { get; set; }
    public string ApprovedDate { get; set; }
}
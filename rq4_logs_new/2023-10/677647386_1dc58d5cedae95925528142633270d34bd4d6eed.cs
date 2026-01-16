namespace TsdDelivery.Application.Commons;

public class ZaloPayConfig
{
    public  string AppUser {get; set; }  = string.Empty;
    public  string PaymentUrl {get; set; } = string.Empty;
    public  string RedirectUrl {get; set; } = string.Empty;
    public  string IpnUrl {get; set; } = string.Empty;
    public  string CallbackUrl {get; set; } = string.Empty;
    
    public  int AppId {get; set; } 
    public string Key1 { get; set; } = string.Empty;
    public string Key2 { get; set; } = string.Empty;
}
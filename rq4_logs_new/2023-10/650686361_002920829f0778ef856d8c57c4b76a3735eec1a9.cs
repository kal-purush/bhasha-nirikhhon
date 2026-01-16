using System.ComponentModel.DataAnnotations;

namespace ShareInvest.Entities;

public class Securities
{
    [StringLength(0x10), Key]
    public string? AccNo
    {
        get; set;
    }
    [StringLength(0x40), Key]
    public string? SerialKey
    {
        get; set;
    }
    [StringLength(0x20)]
    public string? UserName
    {
        get; set;
    }
    [StringLength(0x40)]
    public string? UserId
    {
        get; set;
    }
    [StringLength(0x20), Required]
    public string? SecuritiesId
    {
        get; set;
    }
}
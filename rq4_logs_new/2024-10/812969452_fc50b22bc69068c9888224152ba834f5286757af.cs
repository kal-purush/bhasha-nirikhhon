using NetEscapades.EnumGenerators;
using System.ComponentModel.DataAnnotations;

namespace SmoothNanners.Web.Models;

[EnumExtensions]
public enum BootstrapVariant
{
    [Display(Name = "primary")]
    Primary,

    [Display(Name = "secondary")]
    Secondary,

    [Display(Name = "success")]
    Success,

    [Display(Name = "danger")]
    Danger,

    [Display(Name = "warning")]
    Warning,

    [Display(Name = "info")]
    Info,

    [Display(Name = "light")]
    Light,

    [Display(Name = "dark")]
    Dark
}
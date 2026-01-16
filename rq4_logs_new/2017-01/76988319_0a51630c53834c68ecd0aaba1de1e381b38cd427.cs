using System;
using System.Threading.Tasks;
using Microsoft.Owin;

namespace GoogleRecaptcha.Services
{
    public interface IGoogleRecaptchaNotifications
    {
        Func<IOwinContext, GoogleRecaptchaResponse, Task> InvalidInputResponseNotification { get; set; }
        Func<IOwinContext, GoogleRecaptchaResponse, Task> MissingInputResponseNotification { get; set; }
        Func<IOwinContext, GoogleRecaptchaResponse, Task> ValidInputResponseNotification { get; set; }
        Func<IOwinContext, GoogleRecaptchaResponse, Task> InvalidInputSecretNotification { get; set; }
    }
}
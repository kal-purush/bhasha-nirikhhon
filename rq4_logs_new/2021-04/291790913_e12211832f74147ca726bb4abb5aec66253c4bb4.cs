using LT.DigitalOffice.Kernel.Attributes;
using LT.DigitalOffice.MessageService.Models.Dto.Enums;
using LT.DigitalOffice.UserService.Models.Dto.Configurations;
using System;

namespace LT.DigitalOffice.Broker.Requests
{
    [AutoInjectRequest(nameof(RabbitMqConfig.GetTempalateTagsEndpoint))]
    public interface IGetEmailTemplateTagsRequest
    {
        Guid? TemplateId { get; }
        string Language { get; }
        EmailTemplateType Type { get; }

        static object CreateObj(string language, EmailTemplateType type, Guid? templateId = null)
        {
            return new
            {
                TemplateId = templateId,
                Type = type,
                Language = language,
            };
        }
    }
}
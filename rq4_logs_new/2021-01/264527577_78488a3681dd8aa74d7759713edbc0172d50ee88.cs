// unset

namespace FS.Api.Application.Commands.Command
{
    using MediatR;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using System;
    using Utils.Enums;

    public class CreateInstallmentMovimentCommand : IRequest<Guid>
    {
        public CreateInstallmentMovimentCommand(Guid userId, Guid accountId, string description, 
            decimal value, int months, int startMonth, EMovimentCategory category, EMovimentType type)
        {
            UserId = userId;
            AccountId = accountId;
            Description = description;
            Value = value;
            Category = category;
            Type = type;
            StartMonth = startMonth;
            Months = months;
        }
        
        public Guid UserId { get; }
        public Guid AccountId { get; }
        public string Description { get; }
        public decimal Value { get; }
        public int Months { get; }
        
        public int StartMonth { get; }

        [JsonConverter(typeof(StringEnumConverter))]
        public EMovimentCategory Category { get; }

        [JsonConverter(typeof(StringEnumConverter))]
        public EMovimentType Type { get; }
    }
}
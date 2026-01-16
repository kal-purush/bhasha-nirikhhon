// unset

namespace FS.Api.Application.Commands.Command
{
    using MediatR;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using System;
    using Utils.Enums;

    public class CreateInvestmentCommand : IRequest<Guid>
    {
        public CreateInvestmentCommand(Guid userId, Guid accountId, string description, decimal value,
            EInvestmentType type)
        {
            UserId = userId;
            AccountId = accountId;
            Description = description;
            Value = value;
            Type = type;
        }

        public Guid UserId { get; }
        public Guid AccountId { get; }
        public string Description { get; }
        public decimal Value { get; }

        [JsonConverter(typeof(StringEnumConverter))]
        public EInvestmentType Type { get; }
    }
}
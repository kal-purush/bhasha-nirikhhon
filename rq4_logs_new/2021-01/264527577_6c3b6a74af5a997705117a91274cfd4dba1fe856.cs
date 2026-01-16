namespace FS.Api.Application.Commands.Command
{
    using MediatR;
    using Newtonsoft.Json;
    using Newtonsoft.Json.Converters;
    using System;
    using Utils.Enums;

    public class EditInvestmentCommand : IRequest<Guid>
    {
        public EditInvestmentCommand(Guid userId, Guid accountId, Guid investmentId, string description, decimal value,
            EInvestmentType type)
        {
            UserId = userId;
            AccountId = accountId;
            InvestmentId = investmentId;
            Description = description;
            Value = value;
            Type = type;
        }

        public Guid UserId { get; }
        public Guid AccountId { get; }
        public Guid InvestmentId { get; }
        public string Description { get; }
        public decimal Value { get; }

        [JsonConverter(typeof(StringEnumConverter))]
        public EInvestmentType Type { get; }
    }
}
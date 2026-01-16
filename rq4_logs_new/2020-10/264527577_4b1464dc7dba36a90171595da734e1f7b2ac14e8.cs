namespace FS.Api.Commands.Command
{
    using System;
    using DataObject.Expense.Response;
    using MediatR;

    public class CreateExpenseCommand : IRequest<Guid>
    {
        public CreateExpenseCommand(Guid userId, Guid accountId, string description, decimal value, int category)
        {
            UserId = userId;
            AccountId = accountId;
            Description = description;
            Value = value;
            Category = category;
        }

        public Guid UserId { get; }
        public Guid AccountId { get; }
        public string Description { get; }
        public decimal Value { get; }
        public int Category { get; }
    }
}
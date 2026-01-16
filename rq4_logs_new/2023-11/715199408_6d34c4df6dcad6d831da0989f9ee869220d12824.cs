using FluentValidation;
using NerdStore.Core.Messages;

namespace NerdStore.Sales.Application.Commands;

public class AddOrderItemCommand : Command
{
    private const int MinimumQuantity = 0;
    private const int MaximumQuantity = 15;
    private const int MinimumValue = 0;

    public Guid CustomerId { get; private set; }
    public Guid ProductId { get; private set; }
    public string Name { get; private set; }
    public int Quantity { get; private set; }
    public decimal UnitValue { get; private set; }

    public AddOrderItemCommand(
        Guid customerId,
        Guid productId,
        string name,
        int quantity,
        decimal unitValue)
    {
        CustomerId = customerId;
        ProductId = productId;
        Name = name;
        Quantity = quantity;
        UnitValue = unitValue;
    }

    public override bool IsValid()
    {
        ValidationResult = new AddOrderItemValidation().Validate(this);

        return ValidationResult.IsValid;
    }

    public class AddOrderItemValidation : AbstractValidator<AddOrderItemCommand>
    {
        public AddOrderItemValidation()
        {
            RuleFor(c => c.CustomerId)
                .NotEqual(Guid.Empty)
                    .WithMessage("Id do cliente inválido");

            RuleFor(c => c.ProductId)
                .NotEqual(Guid.Empty)
                    .WithMessage("Id do produto inválido");

            RuleFor(c => c.Name)
                .NotEmpty()
                    .WithMessage("O nome do produto não foi informado");

            RuleFor(c => c.Quantity)
                .GreaterThan(MinimumQuantity)
                    .WithMessage("A quantidade mínima de um item é 1");

            RuleFor(c => c.Quantity)
                .LessThan(MaximumQuantity)
                    .WithMessage("A quantidade máximma de um item é 15");

            RuleFor(c => c.UnitValue)
                .GreaterThan(MinimumValue)
                    .WithMessage("O valor do item precisa ser maior que 0");
        }
    }
}
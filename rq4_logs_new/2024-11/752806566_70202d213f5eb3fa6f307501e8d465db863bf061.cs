namespace GSOP.Domain.Contracts.ProductionLines.ProductionRules
{
    public record CalibratoinChangeRuleDTO
    {
        public required double CalibrationTo { get; init; }

        public required TimeSpan ChangeTime { get; init; }

        public required double ChangeConsumption { get; init; }
    }
}
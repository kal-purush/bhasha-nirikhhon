namespace WageCalculator;

/// <summary>
/// Represents the calculated results of a wage calculation, detailing gross and net amounts, contributions, taxes, and a breakdown of taxes by brackets.
/// </summary>
public class CalculatedWage
{
    /// <summary>
    /// Gets or sets the gross wage calculated. This is the total wage before any deductions such as taxes and contributions.
    /// </summary>
    public decimal Gross { get; set; }

    /// <summary>
    /// Gets or sets the net wage calculated. This is the wage after all deductions, including taxes and contributions, have been applied.
    /// </summary>
    public decimal Net { get; set; }

    /// <summary>
    /// Gets or sets the total contributions deducted from the gross wage. This typically includes items like pension contributions and health insurance.
    /// </summary>
    public decimal Contribution { get; set; }

    /// <summary>
    /// Gets or sets the total tax deducted from the gross wage. This is the sum of taxes across all applicable tax brackets.
    /// </summary>
    public decimal Tax { get; set; }

    /// <summary>
    /// Gets or sets a list of <see cref="TaxBracketBreakDown"/> objects that provide a detailed breakdown of taxes applied at different income thresholds.
    /// </summary>
    public List<TaxBracketBreakDown> BreakDown { get; set; } = new();
}
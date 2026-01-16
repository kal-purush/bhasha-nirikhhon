namespace WageCalculator;

/// <summary>
/// Represents a member of a health insurance plan.
/// </summary>
/// <typeparam name="T">The type of the identifier used for the health insurance member.</typeparam>
public class HealthInsuranceMember<T>
{
    /// <summary>
    /// Gets or sets the identifier for the health insurance member.
    /// </summary>
    /// <value>The identifier of the member.</value>
    public T Id { get; set; } = default!;

    /// <summary>
    /// Gets or sets the prime, or primary monetary amount, associated with the health insurance member.
    /// </summary>
    /// <value>The prime amount as a decimal.</value>
    public decimal Prime { get; set; }
}
namespace Builder.Abstractions;

/// <summary>
/// Represents the IMechanicalKeyboardBuilder interface.
/// </summary>
internal interface IMechanicalKeyboardBuilder
{
    /// <summary>
    /// Sets value for Case property.
    /// </summary>
    /// <param name="case">The case parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for Case property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetCase(Case? @case);

    /// <summary>
    /// Sets value for PCB property.
    /// </summary>
    /// <param name="pcb">The pcb parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for PCB property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetPCB(PCB? pcb);

    /// <summary>
    /// Sets value for Color property.
    /// </summary>
    /// <param name="color">The color parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for Color property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetColor(string? color);

    /// <summary>
    /// Sets value for Plate property.
    /// </summary>
    /// <param name="plate">The plate parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for Plate property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetPlate(Plate? plate);

    /// <summary>
    /// Sets value for Switch property.
    /// </summary>
    /// <param name="switch">The switch parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for Switch property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetSwitch(Switch? @switch);

    /// <summary>
    /// Sets value for Keycap property.
    /// </summary>
    /// <param name="keycap">The keycap parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for Keycap property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetKeycap(Keycap? keycap);

    /// <summary>
    /// Sets value for KeycapProfile property.
    /// </summary>
    /// <param name="keycapProfile">The keycapProfile parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for KeycapProfile property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetKeycapProfile(KeycapProfile? keycapProfile);

    /// <summary>
    /// Sets value for Stabilizer property.
    /// </summary>
    /// <param name="stabilizer">The stabilizer parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for Stabilizer property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetStabilizer(Stabilizer? stabilizer);

    /// <summary>
    /// Sets value for Led property.
    /// </summary>
    /// <param name="led">The led parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for Led property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetLed(Led? led);

    /// <summary>
    /// Sets value for ConnectionSupport property.
    /// </summary>
    /// <param name="connectionSupport">The connectionSupport parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for ConnectionSupport property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetConnectionSupport(ConnectionSupport? connectionSupport);

    /// <summary>
    /// Sets value for Layout property.
    /// </summary>
    /// <param name="layout">The layout parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for Layout property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetLayout(Layout? layout);

    /// <summary>
    /// Sets for IsHaveKnob property.
    /// </summary>
    /// <param name="isHaveKnob">The isHaveKnob parameter.</param>
    /// <returns>
    /// A new instance of <see cref="MechanicalKeyboardBuilder"/> with assigned value for IsHaveKnob property.
    /// </returns>
    internal MechanicalKeyboardBuilder SetIsHaveKnob(bool isHaveKnob);

    /// <summary>
    /// Generate a random code with 6 digits of characters and numbers. 
    /// </summary>
    /// <returns>The random code.</returns>
    internal string GenerateRandomCode();
}
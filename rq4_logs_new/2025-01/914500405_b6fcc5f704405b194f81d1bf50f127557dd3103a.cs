namespace Builder;

/// <summary>
/// Represents the <see cref="Director"/> class.
/// The Director is only responsible for executing
/// the building steps in a particular sequence.
/// </summary>
internal sealed class Director(IMechanicalKeyboardBuilder builder)
{
    /// <summary>
    /// The _builder variable.
    /// </summary>
    private readonly IMechanicalKeyboardBuilder _builder = builder;

    /// <summary>
    /// Build standard mechanical keyboard processes.
    /// </summary>
    internal void BuildStandardMechanicalKeyboard()
    {
        _builder.SetCase(new Case(_builder.GenerateRandomCode(), CaseType.Plastic.GetDescription()))
                .SetPCB(new PCB(_builder.GenerateRandomCode(), PCBType.Solder.GetDescription()))
                .SetColor(Color.Black.GetDescription())
                .SetPlate(new Plate(_builder.GenerateRandomCode(), PlateType.PC.GetDescription()))
                .SetSwitch(new Switch(_builder.GenerateRandomCode(), SwitchType.Clicky.GetDescription()))
                .SetKeycap(new Keycap(_builder.GenerateRandomCode(), KeycapType.ABS.GetDescription()))
                .SetKeycapProfile(
                    new KeycapProfile(_builder.GenerateRandomCode(), KeycapProfileType.OEM.GetDescription()))
                .SetStabilizer(new Stabilizer(_builder.GenerateRandomCode(), StabilizerType.Costar.GetDescription()))
                .SetConnectionSupport(
                    new ConnectionSupport(
                        _builder.GenerateRandomCode(),
                        ConnectionType.Wire.GetDescription()))
                .SetLayout(new Layout(_builder.GenerateRandomCode(), LayoutType.FullSize.GetDescription()))
                .SetIsHaveKnob(false);
    }

    /// <summary>
    /// Build premium mechanical keyboard processes.
    /// </summary>
    internal void BuildPremiumMechanicalKeyboard()
    {
        _builder.SetCase(new Case(_builder.GenerateRandomCode(), CaseType.Metal.GetDescription()))
                .SetPCB(new PCB(_builder.GenerateRandomCode(), PCBType.Hotswap.GetDescription()))
                .SetColor(Color.Purple.GetDescription())
                .SetPlate(new Plate(_builder.GenerateRandomCode(), PlateType.Metal.GetDescription()))
                .SetSwitch(new Switch(_builder.GenerateRandomCode(), SwitchType.Linear.GetDescription()))
                .SetKeycap(new Keycap(_builder.GenerateRandomCode(), KeycapType.PBT.GetDescription()))
                .SetKeycapProfile(
                    new KeycapProfile(_builder.GenerateRandomCode(), KeycapProfileType.DSA.GetDescription()))
                .SetStabilizer(new Stabilizer(_builder.GenerateRandomCode(), StabilizerType.Cherry.GetDescription()))
                .SetLed(new Led(_builder.GenerateRandomCode(), LedType.RGB.GetDescription()))
                .SetConnectionSupport(
                    new ConnectionSupport(
                        _builder.GenerateRandomCode(),
                        ConnectionType.BothWireAndWireless.GetDescription()))
                .SetLayout(new Layout(_builder.GenerateRandomCode(), LayoutType.SeventyFive.GetDescription()))
                .SetIsHaveKnob(true);
    }
}
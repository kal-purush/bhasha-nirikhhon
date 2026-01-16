using FluentAssertions;

namespace ReplaceConditionalLogicWithStrategy.Tests;

[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public class TestFarmLand
{
    private readonly FarmLand _farm;

    public TestFarmLand()
    {
        _farm = new FarmLand();
    }
    
    [Test]
    public void farm_land_starts_as_a_garden()
    {
        _farm.Describe().Should().Be("Garden");
    }

    [Test]
    public void a_farm_land_s_type_can_be_changed_at_run_time()
    {
        _farm.Describe().Should().Be("Garden");
        _farm.SetType(Type.Field);
        
        _farm.Describe().Should().Be("Field");
    }

    [Test]
    public void a_garden_describes_itself_as_a_garden()
    {
        _farm.SetType(Type.Garden);
        _farm.Describe().Should().Be("Garden");
    }

    [Test]
    public void a_garden_yields_20_with_enough_workers()
    {
        _farm.SetType(Type.Garden);
        var yield = _farm.CalculateYield(6);
        yield.Should().Be(20);
    }
    
    [Test]
    public void a_garden_yields_0_2_if_not_enough_workers()
    {
        _farm.SetType(Type.Garden);
        var yield = _farm.CalculateYield(3);
        yield.Should().Be(0.2);
    }

    [Test]
    public void a_field_describes_itself_as_a_field()
    {
        _farm.SetType(Type.Field);
        _farm.Describe().Should().Be("Field");
    }

    [Test]
    public void a_field_yields_4_with_enough_rain()
    {
        _farm.SetType(Type.Field);
        var yield = _farm.CalculateYield(1, 12);
        yield.Should().Be(4);
    }
    
    [Test]
    public void a_field_yields_2_if_medium_amount_of_rain()
    {
        _farm.SetType(Type.Field);
        var yield = _farm.CalculateYield(1, 6);
        yield.Should().Be(2);
    }
    
    [Test]
    public void a_field_yields_1_if_not_enough_rain()
    {
        _farm.SetType(Type.Field);
        var yield = _farm.CalculateYield(1, 3);
        yield.Should().Be(1);
    }
    
    [Test]
    public void an_orchard_describes_itself_as_an_orchard()
    {
        _farm.SetType(Type.Orchard);
        _farm.Describe().Should().Be("Orchard");
    }

    [Test]
    public void an_orchard_yields_10_with_enough_workers_and_rain()
    {
        _farm.SetType(Type.Orchard);
        var yield = _farm.CalculateYield(3, 6);
        yield.Should().Be(10);
    }
    
    [Test]
    public void an_orchard_yields_2_with_enough_workers_but_too_little_rain()
    {
        _farm.SetType(Type.Orchard);
        var yield = _farm.CalculateYield(3, 3);
        yield.Should().Be(2);
    }
    
    [Test]
    public void an_orchard_yields_0_if_not_enough_workers()
    {
        _farm.SetType(Type.Orchard);
        var yield = _farm.CalculateYield(1);
        yield.Should().Be(0);
    }
}
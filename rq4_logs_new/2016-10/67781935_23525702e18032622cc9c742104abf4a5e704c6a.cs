using FluentAssertions;

using KataNetHack.Console;
using KataNetHack.Console.PlayerSubsystem;
using KataNetHack.Console.Renderer;

using Xunit;

namespace KataNetHack.Tests
{
    public class RenderableFactoryTests
    {
        [Fact]
        public void CreateRenderable_Player_ReturnsExpectedRenderable()
        {
            var sut = new RenderableFactory();

            var player = new Player(x: 3, y: 4);
            Renderable result = sut.CreateRenderable(player);

            result.Source.Should().BeSameAs(player);
            result.Representation.Should().Be(RenderableFactory.PLAYER_REPRESENTATION);
            result.ZIndex.Should().Be(RenderableFactory.PLAYER_ZINDEX);
        }
    }
}
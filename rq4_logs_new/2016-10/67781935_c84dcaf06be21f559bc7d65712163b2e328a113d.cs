using KataNetHack.Console;
using KataNetHack.Console.Input;
using KataNetHack.Console.Maps;
using KataNetHack.Console.PlayerSubsystem;
using KataNetHack.Console.Renderer;

namespace KataNetHack.Tests
{
    public class GameEngineBuilder
    {
        private IInput _input = new InputDouble();
        private Player _player = new Player(1, 1);
        private Console.Renderer.Renderer _renderer;
        private IMap _map;
        private RenderableFactory _renderableFactory = new RenderableFactory();

        public GameEngine Build()
        {
            if (_map == null)
            {
                _map = new Stage1().LoadMap();
            }

            if (_renderer == null)
            {
                _renderer = new RendererDouble(_map);
            }

            return new GameEngine(_input, _player, _renderer, _map, _renderableFactory);
        }

        public GameEngineBuilder WithInput(InputDouble input)
        {
            _input = input;

            return this;
        }

        public GameEngineBuilder WithPlayer(Player player)
        {
            _player = player;

            return this;
        }

        public GameEngineBuilder WithRenderer(Console.Renderer.Renderer renderer)
        {
            _renderer = renderer;

            return this;
        }
    }
}
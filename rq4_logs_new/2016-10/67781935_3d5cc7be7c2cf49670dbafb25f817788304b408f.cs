using KataNetHack.Console.Input;
using KataNetHack.Console.PlayerSubsystem;

namespace KataNetHack.Console
{
    public class GameEngine
    {
        private readonly IInput _input;
        private readonly Player _player;

        public GameEngine(IInput input, Player player)
        {
            _input = input;
            _player = player;

            _input.InputReceived += HandleInputReceived;
        }

        private void HandleInputReceived(InputResult inputResult)
        {
            switch (inputResult)
            {
                case InputResult.Up:
                    _player.MoveNorth();
                    break;
                default:
                    break;
            }
        }
    }
}
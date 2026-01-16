using System;
using System.Windows.Input;

namespace KeyboardShortcutDetector.Tests.Fixture
{
    internal class KeyboardMock : IKeyboard
    {
        private KeyboardState _state = KeyboardState.Empty();

        public void Dispose()
        {
        }

        public event Action<KeyboardState> StateChanged;

        public void PressKey(Key key)
        {
            _state = _state.KeyWasPressed(key);
            StateChanged?.Invoke(_state);
        }

        public void ReleaseKey(Key key)
        {
            _state = _state.KeyWasReleased(key);
            StateChanged?.Invoke(_state);
        }
    }
}
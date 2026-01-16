using System;
using KeyboardShortcutDetector.Shortcuts;

namespace KeyboardShortcutDetector
{
    public interface IShortcutObserver<in TShortcut> :
        IShortcutPressedObserver<TShortcut>,
        IShortcutReleasedObserver<TShortcut>
        where TShortcut :class, IKeyboardShortcut
    {}

    public class ActionShortcutObserver<TShortcut> :
        IShortcutObserver<TShortcut>
        where TShortcut : class, IKeyboardShortcut
    {
        private readonly Action<TShortcut> _toExecuteOnReleased;
        private readonly Action<TShortcut> _toExecuteOnPressed;

        public ActionShortcutObserver(Action<TShortcut> toExecute)
        {
            _toExecuteOnPressed = toExecute;
            _toExecuteOnReleased = toExecute;
        }

        public ActionShortcutObserver(
            Action<TShortcut> toExecuteOnReleased,
            Action<TShortcut> toExecuteOnPressed)
        {
            _toExecuteOnReleased = toExecuteOnReleased;
            _toExecuteOnPressed = toExecuteOnPressed;
        }

        public void ShortcutPressed(TShortcut shortcut)
        {
            _toExecuteOnPressed(shortcut);
        }

        public void ShortcutReleased(TShortcut shortcut)
        {
            _toExecuteOnReleased(shortcut);
        }
    }

}
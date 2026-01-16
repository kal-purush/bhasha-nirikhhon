using System;
using KeyboardShortcutDetector.Shortcuts;

namespace KeyboardShortcutDetector
{
    public interface IShortcutPressedObserver<in TShortcut> 
        where TShortcut :class, IKeyboardShortcut
    {
        void ShortcutPressed(TShortcut shortcut);
    }

    public class ActionShortcutPressedObserver<TShortcut> : 
        IShortcutPressedObserver<TShortcut>
        where TShortcut : class, IKeyboardShortcut
    {
        private readonly Action<TShortcut> _toExecute;

        public ActionShortcutPressedObserver(Action<TShortcut> toExecute)
        {
            if (toExecute == null) throw new ArgumentNullException(nameof(toExecute));
            _toExecute = toExecute;
        }

        public void ShortcutPressed(TShortcut shortcut)
        {
            _toExecute(shortcut);
        }
    }


}
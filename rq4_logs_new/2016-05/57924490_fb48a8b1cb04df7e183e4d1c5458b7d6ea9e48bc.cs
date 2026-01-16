using System;
using KeyboardShortcutDetector.Shortcuts;

namespace KeyboardShortcutDetector
{
    public static class ShortcutDetectorExtensions
    {
        internal static void RestartOnCtrlAltDel(this IKeyboardShortcutDetector detector)
        {
            detector.RegisterShortcut(new CtrlAltDelShortcut());
            detector.Subscribe(new ActionShortcutPressedObserver<CtrlAltDelShortcut>(shortcut => detector.Restart()));
        }

        public static void ExecuteOnPressed<TShortcut>(this IKeyboardShortcutDetector detector, Action<TShortcut> action)
            where TShortcut : class, IKeyboardShortcut
        {
            detector.Subscribe(new ActionShortcutPressedObserver<TShortcut>(action));
        }

        public static void ExecuteOnPressed<TShortcut>(this IKeyboardShortcutDetector detector, Action action)
            where TShortcut : class, IKeyboardShortcut
        {
            detector.Subscribe(new ActionShortcutPressedObserver<TShortcut>(shortcut => action()));
        }

        public static void ExecuteOnReleased<TShortcut>(this IKeyboardShortcutDetector detector, Action<TShortcut> action)
            where TShortcut : class, IKeyboardShortcut
        {
            detector.Subscribe(new ActionShortcutReleasedObserver<TShortcut>(action));
        }

        public static void ExecuteOnReleased<TShortcut>(this IKeyboardShortcutDetector detector, Action action)
            where TShortcut : class, IKeyboardShortcut
        {
            detector.Subscribe(new ActionShortcutReleasedObserver<TShortcut>(shortcut => action()));
        }
        
        public static void ExecuteOnBoth<TShortcut>(this IKeyboardShortcutDetector detector, Action<TShortcut> action)
            where TShortcut : class, IKeyboardShortcut
        {
            detector.Subscribe(new ActionShortcutObserver<TShortcut>(action));
        }

        public static void ExecuteOnBoth<TShortcut>(this IKeyboardShortcutDetector detector, Action action)
            where TShortcut : class, IKeyboardShortcut
        {
            detector.Subscribe(new ActionShortcutObserver<TShortcut>(shortcut => action()));
        }
    }
}
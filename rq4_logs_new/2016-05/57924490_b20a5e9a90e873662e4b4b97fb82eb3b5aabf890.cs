using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;
using FluentAssertions;
using KeyboardShortcutDetector.Shortcuts;
using NUnit.Framework;

namespace KeyboardShortcutDetector.Tests
{
    [TestFixture]
    internal class CtrlAltDelShortcutTests
    {

        [TestCase(
            new[] { Key.LeftCtrl },
            new[] { Key.LeftCtrl, Key.LeftAlt },
            new[] { Key.LeftCtrl, Key.LeftAlt, Key.Delete })]

        [TestCase(
            new[] { Key.LeftCtrl },
            new[] { Key.LeftCtrl, Key.RightAlt },
            new[] { Key.LeftCtrl, Key.RightAlt, Key.Delete })]

        [TestCase(
            new[] { Key.RightCtrl },
            new[] { Key.RightCtrl, Key.LeftAlt },
            new[] { Key.RightCtrl, Key.LeftAlt, Key.Delete })]

        [TestCase(
            new[] { Key.RightCtrl },
            new[] { Key.RightCtrl, Key.RightAlt },
            new[] { Key.RightCtrl, Key.RightAlt, Key.Delete })]

        [TestCase(
            new[] { Key.LeftAlt },
            new[] { Key.LeftAlt, Key.LeftCtrl },
            new[] { Key.LeftAlt, Key.LeftCtrl, Key.Delete })]

        [TestCase(
            new[] { Key.LeftAlt },
            new[] { Key.LeftAlt, Key.Delete },
            new[] { Key.LeftAlt, Key.Delete, Key.LeftCtrl })]

        [TestCase(
            new[] { Key.Delete },
            new[] { Key.Delete, Key.LeftAlt },
            new[] { Key.Delete, Key.LeftAlt, Key.LeftCtrl })]

        [TestCase(
            new[] { Key.Delete },
            new[] { Key.Delete, Key.LeftCtrl },
            new[] { Key.Delete, Key.LeftCtrl, Key.LeftAlt })]

        public void Shortcut_should_be_pressed(Key[] beforePreviousState, Key[] previousState, Key[] currentState)
        {
            var keyboardState = new KeyboardState(
                beforePreviousState.ToImmutableArray(),
                previousState.ToImmutableArray(),
                currentState.ToImmutableArray());

            var shortcut = new CtrlAltDelShortcut();

            shortcut.KeyboardStateChanged(keyboardState).Should().Be(ShortcutStateChange.Pressed);
        }

        [TestCase(
            new[] { Key.LeftCtrl, Key.LeftAlt },
            new[] { Key.LeftCtrl, Key.LeftAlt, Key.Delete },
            new[] { Key.LeftCtrl, Key.LeftAlt })]

        [TestCase(
            new[] { Key.LeftCtrl, Key.RightAlt },
            new[] { Key.LeftCtrl, Key.RightAlt, Key.Delete },
            new[] { Key.LeftCtrl, Key.RightAlt })]

        [TestCase(
            new[] { Key.RightCtrl, Key.LeftAlt },
            new[] { Key.RightCtrl, Key.LeftAlt, Key.Delete },
            new[] { Key.RightCtrl, Key.LeftAlt })]

        public void Shortcut_should_be_released(Key[] beforePreviousState, Key[] previousState, Key[] currentState)
        {
            var keyboardState = new KeyboardState(
                beforePreviousState.ToImmutableArray(),
                previousState.ToImmutableArray(),
                currentState.ToImmutableArray());

            var shortcut = new CtrlAltDelShortcut();

            shortcut.KeyboardStateChanged(keyboardState).Should().Be(ShortcutStateChange.Released);
        }

        [TestCase(
            new[] { Key.LeftCtrl, Key.LeftAlt },
            new[] { Key.LeftCtrl, Key.LeftAlt, Key.Delete },
            new[] { Key.LeftCtrl, Key.Delete })]

        [TestCase(
            new[] { Key.LeftCtrl, Key.RightAlt },
            new[] { Key.LeftCtrl, Key.RightAlt, Key.Delete },
            new[] { Key.LeftCtrl, Key.Delete })]

        [TestCase(
            new[] { Key.RightCtrl, Key.LeftAlt },
            new[] { Key.RightCtrl, Key.LeftAlt, Key.Delete },
            new[] { Key.LeftAlt, Key.Delete })]

        public void Shortcut_should_not_be_released(Key[] beforePreviousState, Key[] previousState, Key[] currentState)
        {
            var keyboardState = new KeyboardState(
                beforePreviousState.ToImmutableArray(),
                previousState.ToImmutableArray(),
                currentState.ToImmutableArray());

            var shortcut = new CtrlAltDelShortcut();

            shortcut.KeyboardStateChanged(keyboardState).Should().Be(ShortcutStateChange.None);
        }

    }
}
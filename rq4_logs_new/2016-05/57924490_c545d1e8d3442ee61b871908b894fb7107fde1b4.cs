using System.Collections.Immutable;
using System.Windows.Input;
using FluentAssertions;
using KeyboardShortcutDetector.Shortcuts;
using NUnit.Framework;

namespace KeyboardShortcutDetector.Tests
{
    [TestFixture]
    internal class CombinationShortcutTests
    {
        [TestCase(
            new Key[] {},
            new[] {Key.LeftCtrl},
            new[] {Key.LeftCtrl, Key.LeftAlt})]

        [TestCase(
            new[] {Key.RightCtrl},
            new[] {Key.RightCtrl, Key.LeftAlt},
            new[] {Key.RightCtrl, Key.LeftAlt, Key.C})]

        [TestCase(
            new[] {Key.LeftCtrl, Key.LeftAlt, Key.C,},
            new[] {Key.LeftCtrl, Key.LeftAlt, Key.C, Key.K},
            new[] {Key.LeftCtrl, Key.LeftAlt, Key.C,})]

        public void Shortcut_should_not_be_pressed(Key[] beforePreviousState, Key[] previousState, Key[] currentState)
        {
            var keyboardState = new KeyboardState(
                beforePreviousState.ToImmutableArray(),
                previousState.ToImmutableArray(),
                currentState.ToImmutableArray());

            var shortcut = new CombinationShortcut(Key.LeftCtrl, Key.LeftAlt, Key.C);

            shortcut.KeyboardStateChanged(keyboardState).Should().Be(ShortcutStateChange.None);
        }

        [TestCase(
            new[] {Key.LeftCtrl},
            new[] {Key.LeftCtrl, Key.LeftAlt},
            new[] {Key.LeftCtrl, Key.LeftAlt, Key.C})]

        [TestCase(
            new[] {Key.LeftCtrl},
            new[] {Key.LeftCtrl, Key.C},
            new[] {Key.LeftCtrl, Key.C, Key.LeftAlt})]

        [TestCase(
            new[] {Key.LeftAlt},
            new[] {Key.LeftAlt, Key.LeftCtrl},
            new[] {Key.LeftAlt, Key.LeftCtrl, Key.C})]

        [TestCase(
            new[] {Key.LeftAlt},
            new[] {Key.LeftAlt, Key.C},
            new[] {Key.LeftAlt, Key.C, Key.LeftCtrl})]

        [TestCase(
            new[] {Key.C},
            new[] {Key.C, Key.LeftCtrl},
            new[] {Key.C, Key.LeftCtrl, Key.LeftAlt})]

        [TestCase(
            new[] {Key.C},
            new[] {Key.C, Key.LeftAlt},
            new[] {Key.C, Key.LeftAlt, Key.LeftCtrl})]

        [TestCase(
            new[] {Key.C, Key.LeftAlt, Key.A},
            new[] {Key.C, Key.LeftAlt},
            new[] {Key.C, Key.LeftAlt, Key.LeftCtrl})]

        public void Shortcut_should_be_pressed(Key[] beforePreviousState, Key[] previousState, Key[] currentState)
        {
            var keyboardState = new KeyboardState(
                beforePreviousState.ToImmutableArray(),
                previousState.ToImmutableArray(),
                currentState.ToImmutableArray());

            var shortcut = new CombinationShortcut(Key.LeftCtrl, Key.LeftAlt, Key.C);

            shortcut.KeyboardStateChanged(keyboardState).Should().Be(ShortcutStateChange.Pressed);
        }

        [TestCase(
            new[] { Key.LeftCtrl, Key.LeftAlt, Key.C, Key.D },
            new[] { Key.LeftCtrl, Key.LeftAlt, Key.C },
            new[] { Key.LeftCtrl, Key.LeftAlt })]

        [TestCase(
            new[] { Key.LeftCtrl, Key.C, Key.LeftAlt, Key.D },
            new[] { Key.LeftCtrl, Key.C, Key.LeftAlt },
            new[] { Key.LeftCtrl, Key.C })]

        [TestCase(
            new[] { Key.LeftAlt, Key.LeftCtrl, Key.C, Key.D},
            new[] { Key.LeftAlt, Key.LeftCtrl, Key.C },
            new[] { Key.LeftAlt, Key.LeftCtrl })]

        [TestCase(
            new[] { Key.LeftAlt, Key.C, Key.LeftCtrl, Key.D },
            new[] { Key.LeftAlt, Key.C, Key.LeftCtrl },
            new[] { Key.LeftAlt, Key.C })]

        [TestCase(
            new[] { Key.C, Key.LeftCtrl, Key.LeftAlt, Key.D },
            new[] { Key.C, Key.LeftCtrl, Key.LeftAlt },
            new[] { Key.C, Key.LeftCtrl })]

        [TestCase(
            new[] { Key.C, Key.LeftAlt, Key.LeftCtrl, Key.D },
            new[] { Key.C, Key.LeftAlt, Key.LeftCtrl },
            new[] { Key.C, Key.LeftAlt })]

        [TestCase(
            new[] { Key.LeftCtrl, Key.LeftAlt },
            new[] { Key.LeftCtrl, Key.LeftAlt, Key.C },
            new[] { Key.LeftCtrl, Key.C })]

        [TestCase(
            new[] { Key.LeftCtrl, Key.C },
            new[] { Key.LeftCtrl, Key.C, Key.LeftAlt },
            new[] { Key.LeftCtrl, Key.LeftAlt })]

        [TestCase(
            new[] { Key.LeftAlt, Key.LeftCtrl },
            new[] { Key.LeftAlt, Key.LeftCtrl, Key.C },
            new[] { Key.LeftAlt, Key.C })]

        [TestCase(
            new[] { Key.LeftAlt, Key.C },
            new[] { Key.LeftAlt, Key.C, Key.LeftCtrl },
            new[] { Key.LeftAlt, Key.LeftCtrl })]

        [TestCase(
            new[] { Key.C, Key.LeftCtrl },
            new[] { Key.C, Key.LeftCtrl, Key.LeftAlt },
            new[] { Key.C, Key.LeftAlt })]

        [TestCase(
            new[] { Key.C, Key.LeftAlt },
            new[] { Key.C, Key.LeftAlt, Key.LeftCtrl },
            new[] { Key.C, Key.LeftCtrl })]

        public void Shortcut_should_not_be_released(Key[] beforePreviousState, Key[] previousState, Key[] currentState)
        {
            var keyboardState = new KeyboardState(
                beforePreviousState.ToImmutableArray(),
                previousState.ToImmutableArray(),
                currentState.ToImmutableArray());

            var shortcut = new CombinationShortcut(Key.LeftCtrl, Key.LeftAlt, Key.C);

            shortcut.KeyboardStateChanged(keyboardState).Should().Be(ShortcutStateChange.None);
        }

        [TestCase(
            new[] {Key.LeftCtrl, Key.LeftAlt},
            new[] {Key.LeftCtrl, Key.LeftAlt, Key.C},
            new[] {Key.LeftCtrl, Key.LeftAlt})]

        [TestCase(
            new[] {Key.LeftCtrl, Key.C},
            new[] {Key.LeftCtrl, Key.C, Key.LeftAlt},
            new[] {Key.LeftCtrl, Key.C})]

        [TestCase(
            new[] {Key.LeftAlt, Key.LeftCtrl},
            new[] {Key.LeftAlt, Key.LeftCtrl, Key.C},
            new[] {Key.LeftAlt, Key.LeftCtrl})]

        [TestCase(
            new[] {Key.LeftAlt, Key.C},
            new[] {Key.LeftAlt, Key.C, Key.LeftCtrl},
            new[] {Key.LeftAlt, Key.C})]

        [TestCase(
            new[] {Key.C, Key.LeftCtrl},
            new[] {Key.C, Key.LeftCtrl, Key.LeftAlt},
            new[] {Key.C, Key.LeftCtrl})]

        [TestCase(
            new[] {Key.C, Key.LeftAlt},
            new[] {Key.C, Key.LeftAlt, Key.LeftCtrl},
            new[] {Key.C, Key.LeftAlt})]

        public void Shortcut_should_be_released(Key[] beforePreviousState, Key[] previousState, Key[] currentState)
        {
            var keyboardState = new KeyboardState(
                beforePreviousState.ToImmutableArray(),
                previousState.ToImmutableArray(),
                currentState.ToImmutableArray());

            var shortcut = new CombinationShortcut(Key.LeftCtrl, Key.LeftAlt, Key.C);

            shortcut.KeyboardStateChanged(keyboardState).Should().Be(ShortcutStateChange.Released);
        }
    }
}
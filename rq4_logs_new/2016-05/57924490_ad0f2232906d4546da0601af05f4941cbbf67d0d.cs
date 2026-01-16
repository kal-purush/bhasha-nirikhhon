using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Windows.Input;

namespace KeyboardShortcutDetector
{
    public class SeriesShortcut : IKeyboardShortcut
    {
        private readonly ImmutableArray<Key> _keys;
        private readonly ImmutableArray<Key> _lastKeyPossbileValues;

        public SeriesShortcut(IEnumerable<Key> keys, IEnumerable<Key> lastKeyPossibleValues)
        {
            _keys = keys.ToImmutableArray();
            _lastKeyPossbileValues = lastKeyPossibleValues.ToImmutableArray();
        }

        public Key? LastKey { get; private set; }

        public virtual ShortcutStateChange KeyboardStateChanged(KeyboardState state)
        {
            if (WasPressed(state))
            {
                LastKey = state.CurrentCombination.Last();
                return ShortcutStateChange.Pressed;
            }

            if (WasReleased(state))
            {
                LastKey = state.PreviousCombination.Last();
                return ShortcutStateChange.Released;
            }
            
            LastKey = null;
            return ShortcutStateChange.None;
        }

        protected virtual bool WasPressed(KeyboardState state)
        {
            return Enumerable.SequenceEqual(state.PreviousCombination, _keys) &&
                   Enumerable.SequenceEqual(state.CurrentCombination.SkipLast(), _keys) &&
                   _lastKeyPossbileValues.Contains(state.CurrentCombination.Last());
        }

        protected virtual bool WasReleased(KeyboardState state)
        {
            return Enumerable.SequenceEqual(state.BeforePreviousCombination, _keys) &&
                   Enumerable.SequenceEqual(state.PreviousCombination.SkipLast(), _keys) &&
                   _lastKeyPossbileValues.Contains(state.PreviousCombination.Last()) &&
                   Enumerable.SequenceEqual(state.CurrentCombination, _keys);
        }
    }
}
using System.Collections;
using System.Runtime.CompilerServices;
using Engine.Models.Moves;

namespace Engine.DataStructures.Moves.Lists
{

    public abstract class MoveBaseList<T> : IEnumerable<T> where T : MoveBase
    {
        protected readonly T[] _items;

        protected MoveBaseList() : this(128)
        {
        }

        public T this[int i]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return _items[i];
            }
        }

        #region Implementation of IReadOnlyCollection<out IMove>

        public int Count;

        protected MoveBaseList(int capacity)
        {
            _items = new T[capacity];
        }

        #endregion


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Add(T move)
        {
            _items[Count++] = move;
        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear()
        {
            Count = 0;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public IEnumerator<T> GetEnumerator()
        {
            for (int i = 0; i < Count; i++)
            {
                yield return _items[i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public override string ToString()
        {
            return $"Count={Count}";
        }
    }
}
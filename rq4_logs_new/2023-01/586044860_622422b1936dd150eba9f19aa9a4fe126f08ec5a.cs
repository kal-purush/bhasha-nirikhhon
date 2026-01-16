using Engine.Models.Boards;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;

namespace Engine.DataStructures
{
    public  class SquareList
    {
        private readonly Square[] _squares;

        public SquareList()
        {
            _squares = new Square[10];
        }

        public int Length;

        public Square this[int i]
        {
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            get
            {
                return _squares[i];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Clear() { Length = 0; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal void Add(Square square)
        {
            _squares[Length++] = square;
        }
    }
}
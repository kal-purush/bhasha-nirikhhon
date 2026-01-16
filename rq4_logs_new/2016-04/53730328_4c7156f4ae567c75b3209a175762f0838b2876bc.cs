using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SkillsTest.GZipTest.Core.Classes
{
    public class BlockBuffer
    {
        public readonly object SyncRoot = new object();
        private HashSet<PieceOf> buffer = new HashSet<PieceOf>();

        public uint Count { get { return (uint)this.buffer.Count; } }
        public ulong BufferSize { get; private set; }

        public void Add(PieceOf value)
        {
            lock (this.SyncRoot)
            {
                this.buffer.Add(value);
                this.BufferSize += value.Length();
            }
        }

        public HashSet<PieceOf> Fetch(uint count = 1)
        {
            var result = new HashSet<PieceOf>();

            lock (this.SyncRoot)
            {
                result.UnionWith(this.buffer.OrderBy(x => x.SeqNo).Take((int)count));

                this.buffer.ExceptWith(result);
                this.BufferSize -= (ulong)result.Sum(x => (long)x.Length());
            }

            return result;
        }
    }
}
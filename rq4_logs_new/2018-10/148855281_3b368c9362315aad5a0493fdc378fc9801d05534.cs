using System;
using System.IO;

namespace SymbioticTS.Core.IOAbstractions
{
    internal class DisposeActionMemoryStream : MemoryStream
    {
        private readonly Action<MemoryStream> disposeAction;

        private bool disposed = false;

        /// <summary>
        /// Initializes a new instance of the <see cref="DisposeActionMemoryStream"/> class.
        /// </summary>
        /// <param name="disposeAction">The dispose action.</param>
        public DisposeActionMemoryStream(Action<MemoryStream> disposeAction)
        {
            this.disposeAction = disposeAction;
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="T:System.IO.MemoryStream"></see> class and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing">true to release both managed and unmanaged resources; false to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && !this.disposed)
            {
                this.disposeAction?.Invoke(this);
            }

            this.disposed = true;

            base.Dispose(disposing);
        }
    }
}
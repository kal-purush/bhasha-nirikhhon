using System;

namespace Bands.Output
{
    /// <summary>
    /// Helper class for wrapping some inner functionality with a console writer. The IWritable.ToString()
    /// result will be passed to the output before and after inner functionality is executed.
    /// </summary>
    /// <typeparam name="TPayload"></typeparam>
    public class ConsoleWriterBand<TPayload> : Band<TPayload> where TPayload : WritablePayload
    {
        public ConsoleWriterBand(Action<TPayload> action) : base(action) { }

        public ConsoleWriterBand(IBand<TPayload> innerBand) : base(innerBand) { }

        /// <summary>
        /// Output <paramref name="payload"/>.ToString() method prior to and after executing 
        /// inner bands and/or wrapped functionality
        /// </summary>
        /// <param name="payload">An IWritable.ToString()</param>
        public override void Run(TPayload payload)
        {
            Console.WriteLine(payload.ToString());
            InnerBand.Run(payload);
            Console.WriteLine(payload.ToString());
        }
    }
}
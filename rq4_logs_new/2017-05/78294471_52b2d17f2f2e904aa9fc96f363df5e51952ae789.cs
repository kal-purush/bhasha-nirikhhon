using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ghosen.FileExtractor
{
    internal static class FileExtractor
    {
        internal static IEnumerable<byte[]> ProcessMessages(IEnumerable<ISO14229.ISO14229Message> messages)
        {
            var ret = new List<byte>();
            bool collectingBytes = false;
            foreach (var msg in messages)
            {
                // todo: be more careful with transferdataresponses, other responses, etc.
                if (collectingBytes)
                {
                    if (msg.Service == ISO14229.ServiceType.TransferData)
                    {
                        ret.AddRange(((ISO14229.Messages.TransferData)msg).DataRecord);
                    }
                    else if (msg.Service == ISO14229.ServiceType.RequestTransferExit)
                    {
                        collectingBytes = false;
                        var copy = ret.ToArray();
                        ret.Clear();
                        yield return copy;
                    }
                }
                else
                {
                    if (msg.Service == ISO14229.ServiceType.RequestDownload)
                    {
                        collectingBytes = true;
                        //((ISO14229.Messages.RequestDownload)msg);
                    }
                }
            }
            yield break;
        }
    }
}
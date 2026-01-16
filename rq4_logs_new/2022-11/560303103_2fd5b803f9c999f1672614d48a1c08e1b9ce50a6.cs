using System.Security.Cryptography;

namespace EsoZip.Lib;

public static class HashZip
{
    public static byte[]? Decompress(byte[] digest, int fileLength)
    {
        var cts = new CancellationTokenSource();
        byte[]? result = null;
        Parallel.For((long)0, byte.MaxValue,
            new ParallelOptions()
                { CancellationToken = cts.Token, MaxDegreeOfParallelism = Environment.ProcessorCount * 2 },
            i =>
            {
                var sha512 = SHA512.Create();
                var currentTest = Enumerable.Repeat((byte)0, fileLength).ToArray();
                currentTest[0] = (byte)i;
                var found = false;
                while (!found)
                {
                    if (sha512.ComputeHash(currentTest).SequenceEqual(digest))
                    {
                        found = true;
                    }
                    else
                    {
                        IncrementByteArray(currentTest);
                    }
                }

                if (found)
                {
                    result = currentTest;
                    cts.Cancel();
                }
            });
        return result;
    }

    static void IncrementByteArray(byte[] arr)
    {
        int offset = 1;
        while (offset < arr.Length && arr[offset].Equals(byte.MaxValue))
        {
            arr[offset] = 0;
            offset++;
        }
    
        if (offset < arr.Length && arr[offset] < byte.MaxValue)
        {
            arr[offset] += 1;
        }
    }

    public static byte[] Compress(byte[] content)
    {
        var hasher = SHA512.Create();
        return hasher.ComputeHash(content);
    }
}
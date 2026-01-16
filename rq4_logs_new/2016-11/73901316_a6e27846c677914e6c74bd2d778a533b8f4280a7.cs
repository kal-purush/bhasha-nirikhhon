using stuff

namespace __Cereal__{
    class CFTP{
        CFTP(string control, string data){
            byte[] controlBytes = new byte[4]
            System.Buffer.BlockCopy(control.ToCharArray(), 0,
                                    controlBytes, 0, 4);
            int dataSize = data.Length * sizeof(char);
            byte[] dataSizeBytes = new byte[4](BitConverter.GetBytes(dataSize));
            byte[] dataBytes = new byte[dataSize];
            System.Buffer.BlockCopy(data.ToCharArray(), 0,
                                    dataBytes, 0, 4);
            
            byte[] package = new byte[4 + 4 + dataSize];
            package = controlBytes.Concat(dataSizeBytes.Concat(dataBytes).ToArray()).ToArray();
            /*package = controlBytes;
            package.AddRange(dataSizeBytes);
            package.AddRange(dataBytes);*/

            return package;
        }

        CFTP(byte[] package){
            string control = new string();
            string data = new string();
            return [control, data];
        }
    }
}
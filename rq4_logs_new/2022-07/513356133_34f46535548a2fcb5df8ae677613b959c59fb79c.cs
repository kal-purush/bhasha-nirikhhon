using System;
using System.Text;
using System.Collections.Generic;

namespace QuickTools 
{
  public class LowDecrypter
    {
        public static string Data = null;
        public static string DecryptFile(List<int> data)
        {
            //variables needed 

            //int password = passwordInput.GetHashCode();
          //  int salt = LowEncrypter.DataHash * password;
            int foward = LowEncrypter.DataLength;
            // thiw will be the first time it will be decompress 
            byte[] firstDecompress = new byte[LowEncrypter.DataLength];

            for (int value = 0; value < LowEncrypter.DataLength; value++)
            {
                firstDecompress[value] = LowEncrypter.DataBytes[foward-1];
                firstDecompress[value] = Convert.ToByte(firstDecompress[value]^8);
                 

                foward--;
            }
            Data = Encoding.ASCII.GetString(firstDecompress);


            return Encoding.ASCII.GetString(firstDecompress);

        }
    }

}
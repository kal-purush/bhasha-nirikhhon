using System.IO;
using System.Security.Cryptography;
using System.Text;
using ChatApplication.Network;

namespace ChatApplication.Encryption
{
    class Encryption
    {
        byte[] EncodeStringUsingAes(byte[] key, string secretMessage, out byte[] iv)
        {
            using (Aes aes = new AesCryptoServiceProvider()) {
                aes.Key = key;
                iv = aes.IV;

                // Encrypt the message
                using (MemoryStream ciphertext = new MemoryStream())
                using (CryptoStream cs = new CryptoStream(ciphertext, aes.CreateEncryptor(), CryptoStreamMode.Write)) {
                    byte[] plaintextMessage = Encoding.UTF8.GetBytes(secretMessage);
                    cs.Write(plaintextMessage, 0, plaintextMessage.Length);
                    cs.Close();
                    byte[] encryptedMessage = ciphertext.ToArray();
                    return encryptedMessage;
                }
            }

        }

        byte[] EncodeByteArrayUsingAes(byte[] key, byte[] plaintextMessage, out byte[] iv)
        {
            using (Aes aes = new AesCryptoServiceProvider()) {
                aes.Key = key;
                iv = aes.IV;

                // Encrypt the message
                using (MemoryStream ciphertext = new MemoryStream())
                using (CryptoStream cs = new CryptoStream(ciphertext, aes.CreateEncryptor(), CryptoStreamMode.Write)) {
                    cs.Write(plaintextMessage, 0, plaintextMessage.Length);
                    cs.Close();
                    byte[] encryptedMessage = ciphertext.ToArray();
                    return encryptedMessage;
                }
            }

        }

        string DecodeToStringUsingAes(byte[] key, byte[] encryptedMessage, byte[] iv)
        {
            using (Aes aes = new AesCryptoServiceProvider()) {
                aes.Key = key;
                aes.IV = iv;
                // Decrypt the message
                using (MemoryStream plaintext = new MemoryStream()) {
                    using (CryptoStream cs = new CryptoStream(plaintext, aes.CreateDecryptor(), CryptoStreamMode.Write)) {
                        cs.Write(encryptedMessage, 0, encryptedMessage.Length);
                        cs.Close();
                        return Encoding.UTF8.GetString(plaintext.ToArray());
                    }
                }
            }
        }

        byte[] DecodeToByteArrayUsingAes(byte[] key, byte[] encryptedMessage, byte[] iv)
        {
            using (Aes aes = new AesCryptoServiceProvider()) {
                aes.Key = key;
                aes.IV = iv;
                // Decrypt the message
                using (MemoryStream plaintext = new MemoryStream()) {
                    using (CryptoStream cs = new CryptoStream(plaintext, aes.CreateDecryptor(), CryptoStreamMode.Write)) {
                        cs.Write(encryptedMessage, 0, encryptedMessage.Length);
                        cs.Close();
                        return plaintext.ToArray();
                    }
                }
            }
        }

        byte[] PerformAssymetricKeyExchangeUsingECDiffieHellmanOnSocket(System.Net.Sockets.Socket socket)
        {
            using (ECDiffieHellmanCng ECDiffieHellmanCngobj = new ECDiffieHellmanCng()) {

                ECDiffieHellmanCngobj.KeyDerivationFunction = ECDiffieHellmanKeyDerivationFunction.Hash;
                ECDiffieHellmanCngobj.HashAlgorithm = CngAlgorithm.Sha256;
                byte[] _myPublicKey = ECDiffieHellmanCngobj.PublicKey.ToByteArray();
                byte[] _otherPublicKey = new byte[140];

                NetworkCommunicationManagers.SendByteArrayOverSocket(socket, _myPublicKey);
                NetworkCommunicationManagers.ReceiveByteArrayOverSocket(socket, out _otherPublicKey, 140);
                
                return ECDiffieHellmanCngobj.DeriveKeyMaterial(CngKey.Import(_otherPublicKey, CngKeyBlobFormat.EccPublicBlob));
            }
        }
    }
}
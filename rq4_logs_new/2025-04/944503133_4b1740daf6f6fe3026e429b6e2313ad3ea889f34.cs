using Umbraco.Community.EncryptionPropertyEditor.Models;

namespace Umbraco.Community.EncryptionPropertyEditor.Interfaces;

public interface IEncryptionPropertyService
{
    string Hash(string stringToHash, string salt);
    string Encrypt(string stringData, string key, string iv, StringFormat format);
    string Decrypt(string stringData, string key, string iv);

}
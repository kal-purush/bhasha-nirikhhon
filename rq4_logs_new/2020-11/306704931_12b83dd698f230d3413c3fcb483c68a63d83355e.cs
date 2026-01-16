using System.IO;
using UnityEngine;

namespace JevLogin
{
    public sealed class JsonData<T> : IData<T>
    {
        public T Load(string path = null)
        {
            var str = File.ReadAllText(path);
            return JsonUtility.FromJson<T>(UnityEngine.Windows.Crypto.CryptoXOR(str));
        }

        public void Save(T data, string path = null)
        {
            var str = JsonUtility.ToJson(data);
            File.WriteAllText(path, UnityEngine.Windows.Crypto.CryptoXOR(str));
        }
    }
}
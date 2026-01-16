using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using UnityEngine;


namespace JevLogin
{
    public sealed class BinarySerializationData<T> : IData<T>
    {
        private static BinaryFormatter _formatter;

        public BinarySerializationData()
        {
            _formatter = new BinaryFormatter();
        }

        public T Load(string path)
        {
            T result;
            if (!File.Exists(path))
            {
                return default(T);
            }
            using (var fileStream = new FileStream(path, FileMode.Open))
            {
                result = (T)_formatter.Deserialize(fileStream);
            }
            return result;
        }

        public void Save(T data, string path = null)
        {
            if (data == null && !string.IsNullOrEmpty(path))
            {
                return;
            }
            if (!typeof(T).IsSerializable)
            {
                return;
            }
            using (var fileStream = new FileStream(path, FileMode.Create))
            {
                _formatter.Serialize(fileStream, data);
            }
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Serialization;
using System.IO;

namespace Game1
{
    public class XmlManager<T>
    {
        public Type Type
        {
            get;
            set;
        }

        public T Load(string path)
        {
            T instance;
            using (TextReader reader = new StreamReader(path))
            {
                XmlSerializer xmlser = new XmlSerializer(Type);
                instance = (T)xmlser.Deserialize(reader);
            }
            return instance;
        }

        public void Save(string path,object obj)
        {
            using (TextWriter writer = new StreamWriter(path))
            {
                XmlSerializer xmlser = new XmlSerializer(Type);
                xmlser.Serialize(writer, obj);
            }
        }
    }
}
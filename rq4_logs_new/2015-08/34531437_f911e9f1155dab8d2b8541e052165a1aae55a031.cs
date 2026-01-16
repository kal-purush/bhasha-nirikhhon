using System;

namespace LazyStorage.Xml
{
    public class XmlStorage : IStorage
    {
        public XmlStorage(string storageFolder)
        {
            throw new NotImplementedException();
        }

        public IRepository<T> GetRepository<T>() where T : IStorable<T>
        {
            throw new NotImplementedException();
        }

        public void Save()
        {
            throw new NotImplementedException();
        }

        public void Discard()
        {
            throw new NotImplementedException();
        }
    }
}
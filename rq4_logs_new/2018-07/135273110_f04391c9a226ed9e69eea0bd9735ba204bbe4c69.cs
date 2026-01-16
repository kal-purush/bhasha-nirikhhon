using System;
using CAFU.Generics.Data.Entity;

namespace CAFU.Flow.Example.Data.Entity
{
    [Serializable]
    public class Sample1EntityList : ScriptableObjectGenericEntityList<Sample1Entity>
    {
    }

    [Serializable]
    public class Sample1Entity : IGenericEntity
    {
        public int Id;
        public string Name;
    }
}
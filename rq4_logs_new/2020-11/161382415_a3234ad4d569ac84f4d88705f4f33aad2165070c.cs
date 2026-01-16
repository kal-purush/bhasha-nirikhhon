using System;
using System.Collections.Generic;
using System.Linq;
using SolarLunarName.SharedTypes.Models;
using SolarLunarName.SharedTypes.Interfaces;
using System.IO;
using SolarLunarName.SharedTypes.Primitives;
using Newtonsoft.Json;

namespace SolarLunarName.Standard.RestServices.Json
{

    public abstract class Client
    {

        protected abstract T StreamDeligate<T>(ValidYear year, Func<Stream, T> method);

        protected T Deserialize<T>(Stream s){
                using (StreamReader sr = new StreamReader(s))
                using (JsonReader reader = new JsonTextReader(sr))
                {   
                    JsonSerializer serializer = new JsonSerializer();
                    return serializer.Deserialize<T>(reader);
                }
        }

        protected IList<I> DeserializeList<T, I>(Stream s) where T : I{
                return Deserialize<List<T>>(s)
                        .Select(item => (I)item)
                        .ToList();
        }
    }

}
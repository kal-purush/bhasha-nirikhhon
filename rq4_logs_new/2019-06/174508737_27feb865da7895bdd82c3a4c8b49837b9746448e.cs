using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;
using Nexus.Link.Libraries.Core.Error.Model;

namespace Nexus.Link.Libraries.Core.Json
{
    /// <summary>
    /// Convenience methods for JSON stuff.
    /// </summary>
    public class JsonHelper
    {
        public static bool TryDeserializeObject<T>(string value, out T deserializedObject)
        {
            deserializedObject = null;
            var success = false;
            try
            {
                deserializedObject = JsonConvert.DeserializeObject<FulcrumError>();
            }
        }
    }
}
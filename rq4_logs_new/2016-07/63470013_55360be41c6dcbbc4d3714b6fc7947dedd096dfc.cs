using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Paynter.WitAi.Models;

namespace Paynter.WitAi.Collections
{
    public class WitActionDictionary : Dictionary<string, Func<WitConverseRequest, WitConverseResponse, Task<dynamic>>>
    {
        public Func<WitConverseRequest, WitConverseResponse, Task<dynamic>> GetAction(string action)
        {
            return this.FirstOrDefault(u => u.Key.Equals(action, StringComparison.OrdinalIgnoreCase)).Value;
        }
    }
}
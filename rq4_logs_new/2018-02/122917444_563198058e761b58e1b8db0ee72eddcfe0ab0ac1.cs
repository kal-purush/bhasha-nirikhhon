using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    
    class HashmapList<T, E>
    {
        public Dictionary<T, ArrayList> mapList = new Dictionary<T, ArrayList>();

        //insert list of items to the key
        public void add(T key, ArrayList items)
        {
            mapList.Add(key, items);
        }

        //insert item into the list at key
        public void add(T key, E item)
        {
            if (!mapList.ContainsKey(key))
            {
                mapList.Add(key, new ArrayList());
            }
            mapList[key].Add(item);
        }

        //get list of item at key
        public ArrayList get(T key)
        {
            return mapList[key];
        }


        //check if maplist contains key
        public Boolean ContainsKey(T key)
        {
            return mapList.ContainsKey(key);
        }

        //check if list at key contains value
        public Boolean ContainsKeyValue(T key, E value)
        {
            ArrayList values = get(key);
            if (values == null)
            {
                return false;
            }
            return values.Contains(value);
        }

        //get list of keys
        public Dictionary<T, ArrayList>.KeyCollection getKeys()
        {
            return mapList.Keys;
        }




    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.Serialization.Json;
using System.IO;
using System.Collections;

namespace perceptron_web_beeline
{
    class Dictonary_train
    {
        static string config_file_name = "default";
        static DataContractJsonSerializer serializer = new DataContractJsonSerializer(typeof(Dictionary<string, int>));
        public Dictionary<string, int> m_dictonary_map;

        public void SetDict(string dest, int source)
        {
            m_dictonary_map[dest] = source;
        }
        public void SetDict(ref Dictionary<string, int> dict)
        {
            string string_read = "";

            using (MemoryStream ms = new MemoryStream())
            {
                serializer.WriteObject(ms, dict);
                string_read =  Encoding.Default.GetString(ms.ToArray());
            }
            
            m_dictonary_map = GetDictionary(ref string_read);;
        }

        public void SetDict(ref Hashtable has_dict)
        {
            foreach (var key in has_dict.Keys)
            {
                m_dictonary_map[key.ToString()] = (int)has_dict[key];
            }
        }
        public void SetDict(ref List<int> list_dict)
        {
            foreach (var dest in list_dict)
            {
                m_dictonary_map[dest.ToString()] = dest;
            }
        }
        public Dictonary_train(string file_name)
        {
            config_file_name = file_name;
            string string_read = "";
            try
            {
                using (StreamReader sr = File.OpenText(config_file_name))
                {
                    string s = "";
                    if ((s = sr.ReadLine()) != null)
                    {
                        string_read = s;
                    }
                }
                m_dictonary_map = GetDictionary(ref string_read);
            }
            catch (Exception ex) 
            {
                m_dictonary_map = DefaultDictionary();
                Flush();
            }

        }

        public void Flush()
        {
            string json = GetJSON(ref m_dictonary_map);

            try
            {
                using (FileStream fs = File.Create(config_file_name))
                {
                    Byte[] info = new UTF8Encoding(true).GetBytes(json);
                    fs.Write(info, 0, info.Length);
                }
            }
            catch (Exception ex)
            {
                throw new System.Exception("Error: " + ex.Message);
            }

        }


        static public string GetJSON(ref Dictionary<string, int> dict)
        {
            using (MemoryStream ms = new MemoryStream())
            {
                serializer.WriteObject(ms, dict);
                return Encoding.Default.GetString(ms.ToArray());
            }
        }

        static public Dictionary<string, int> GetDictionary(ref string json)
        {
            using (var ms = new MemoryStream(Encoding.Unicode.GetBytes(json)))
            {
                return (Dictionary<string, int>)serializer.ReadObject(ms);
            }
        }
        static public Dictionary<string, int> DefaultDictionary()
        {
            Dictionary<string, int> dict = new Dictionary<string, int>();
            //dict.Add("y", -1000000);
            return dict;
        }
        public int Count()
        {
            int dictonary_count_result;
            dictonary_count_result = m_dictonary_map.Count;
            return dictonary_count_result;
        }
    }
}
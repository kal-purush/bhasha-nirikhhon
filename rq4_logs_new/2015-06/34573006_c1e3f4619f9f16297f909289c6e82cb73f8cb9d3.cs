#region Using statements
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
#endregion

namespace modbrowser
{
    public class Plugin
    {
        public class Api
        {
            public string version { get; set; }
            public string url { get; set; }
        }

        public class Game
        {
            public string name { get; set; }
            public string path { get; set; }
            public string realPath
            {
                get
                {
                    // envVariables[n] -> correspondingPathes[n];
                    string[] envVariables = { "%HOMEPATH%", "%APPDATA%" };
                    string[] correspondingPathes = { Environment.GetFolderPath(Environment.SpecialFolder.UserProfile), Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData) };

                    string newPath = path;

                    for (int i = 0; i < correspondingPathes.Length; i++)
                    {
                        newPath = newPath.Replace(envVariables[i], correspondingPathes[i]);
                    }

                    return newPath;
                }
            }
            public string resourcesPath { get; set; }
            public string modsPrefix { get; set; }
        }

            public string name { get; set; }
            public string author { get; set; }
            public string version { get; set; }
            public Api api { get; set; }
            public Game game { get; set; }
    }
}
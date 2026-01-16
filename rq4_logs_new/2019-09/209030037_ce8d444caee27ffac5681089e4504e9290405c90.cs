using System.Collections;
using System.Collections.Generic;
using System.IO;
using Assets.GameSystem.Constant.Enum;
using UnityEngine;
using Application = UnityEngine.WSA.Application;

namespace Assets.GameSystem.Constant
{
    public abstract class PathConstants
    {
        //Path
        public const string DATA_PATH = "Assets/GameSystem/Data/";
        
        //File
        public const string SETTING_FILE = "Settings.json";

        /// <summary>
        /// Getting File Path
        /// </summary>
        /// <param name="filePath">recieve file path</param>
        /// <returns>Return filepath</returns>
        public static string GetPath(string filePath)
        {
            string file = Path.Combine(filePath);
            if (File.Exists(file))
            {
                return file;
            }

            return DataEnum.DATA_NOT_EXIST.ToString();
        }
    }
}

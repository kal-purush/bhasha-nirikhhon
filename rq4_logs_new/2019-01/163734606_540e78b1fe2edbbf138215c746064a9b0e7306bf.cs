using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using UnityEngine;
using UnityEngine.UI;
using System.Text;
using Harmony;

public static class DuskMod
{
    public static void Init(Text text)
    {
        string curdir = Directory.GetCurrentDirectory();
        string moddir = Path.Combine(curdir, "mods");
        text.text += "\nLoading mods...\n";
        
        try
        {
            var assemblies = Directory.GetFiles(moddir).Where(x => Path.GetExtension(x).ToLower() == ".dll");

            foreach (string file in assemblies)
            {
                text.text += string.Format("Loading mod {0}...\n", Path.GetFileNameWithoutExtension(file));
                var asm = Assembly.LoadFrom(file);
                var types = asm.GetTypes().Where(x => Attribute.IsDefined(x, typeof(ModEntryPoint)));
                foreach (var type in types)
                {
                    var method = type.GetMethod("Main");
                    method.Invoke(null, null);
                }
            }
        }
        catch (Exception e)
        {
            text.text += e.Message + "\n";
        }

        text.text += "\n"; 
    }
}
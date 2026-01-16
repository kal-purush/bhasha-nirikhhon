using System;
using System.Linq;
using UnityEngine;

namespace AzraqamaAerospace
{
    class ModuleKopernicusDisphotovoltaic
    {
        public bool kopernicusLoaded;
        void Awake()
        {
            if (AssemblyLoader.loadedTypes.Any(t => t.Namespace == "Kopernicus" && t.Name == "Injector"))
            {
                kopernicusLoaded = true;
            }
        }
    }
}
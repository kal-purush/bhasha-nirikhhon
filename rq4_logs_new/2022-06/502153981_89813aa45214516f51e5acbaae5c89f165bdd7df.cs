using Autodesk.Revit.DB;
using System.Collections.Generic;

namespace PikTestPlugin.Interfaces
{
    public interface IPaintable
    {
        bool IsPainted { get; }
        void Paint();
    }
}
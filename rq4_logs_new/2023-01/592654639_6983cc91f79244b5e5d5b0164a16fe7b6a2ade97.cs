using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DCTTestAssignment.Models.EventArgsModels;

public class ThemeChanged
{
    public string ThemeName { get; set; }

    public ThemeChanged(string themeName)
    {
        ThemeName = themeName;
    }
}
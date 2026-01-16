using System.ComponentModel;
using Terraria.ModLoader.Config;

namespace TeraCAD
{
    [Label("Config")]
    public class TeraCADConfig : ModConfig
    {
        public override ConfigScope Mode => ConfigScope.ClientSide;

        [Label("Display shpes")]
        [DefaultValue(true)]
        public bool isDrawShpes = true;

        [Label("Display TeraCAD cursor")]
        [DefaultValue(true)]
        public bool isDrawCursor = true;

        [Label("Display TeraCAD cursor snap")]
        [DefaultValue(true)]
        public bool isDrawCursorSnap = true;

        [Label("No border cursor when using TeraCAD tools")]
        [DefaultValue(false)]
        public bool isBorderCursorNone = false;
    }
}
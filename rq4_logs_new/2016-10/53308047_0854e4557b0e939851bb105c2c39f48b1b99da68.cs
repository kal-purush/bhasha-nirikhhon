using System.Reflection;

namespace PlanetarySurfaceStructures
{
    class KPBSSettings : GameParameters.CustomParameterNode
    {
        //[GameParameters.CustomParameterUI("Add custom Filter", autoPersistance = true, toolTip = "Show an additional filter for all the parts of Planetary Base System")]
        //public bool additionalFilter = true;

        [GameParameters.CustomParameterUI("Group all Parts in Function Filter", autoPersistance = true, toolTip = "Group all parts in one category in the function filter")]
        public bool groupParts = false;


        //get the title
        public override string Title {
            get {
                return "Filter Settings";
            }
        }

        //get of the settings have presets
        public override bool HasPresets {
            get {
                return false;
            }
        }

        //get the game mode
        public override GameParameters.GameMode GameMode {
            get {
                return GameParameters.GameMode.ANY;
            }
        }

        //get the section
        public override string Section {
            get {
                return "Planetary Base System";
            }
        }

        //get the section order
        public override int SectionOrder {
            get {
                return 1;
            }
        }


        public override bool Enabled(MemberInfo member, GameParameters parameters)
        {
            return true; 
        }

        public override bool Interactible(MemberInfo member, GameParameters parameters)
        {
            return !HighLogic.LoadedSceneIsEditor;
        }
    }
}
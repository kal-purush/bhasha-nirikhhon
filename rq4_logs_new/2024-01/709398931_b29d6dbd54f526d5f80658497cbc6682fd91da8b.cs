using uml.packages;

namespace uml.extensionsprofile
{
    public class ExtensionsProfileModel : environment.InMemoryModel
    {
        private static ExtensionsProfileModel? instance;

        public Profile Profile_ExtensionsProfile = new();
        public Stereotype Stereotype_ExplicitBaseClassCall = new();

        public static ExtensionsProfileModel Instance()
        {
            if (instance is null)
            {
                instance = new();
                instance.InitializeInMemoryModel();
            }

            return instance;
        }

        private void InitializeInMemoryModel()
        {
            /*
			 * Create in-memory model elements
			 */
            Profile_ExtensionsProfile.SetName("ExtensionsProfile");
            Stereotype_ExplicitBaseClassCall.SetName("ExplicitBaseClassCall");
            Stereotype_ExplicitBaseClassCall._setProfile(Profile_ExtensionsProfile);
        }
    } // ExtensionsProfileModel
}
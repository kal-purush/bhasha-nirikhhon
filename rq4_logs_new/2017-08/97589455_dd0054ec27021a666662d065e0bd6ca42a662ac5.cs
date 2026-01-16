/** This is an automatically generated class by FairyGUI. Please do not modify it. **/

using FairyGUI;
using FairyGUI.Utils;

namespace Login
{
	public partial class CreateRole : GComponent
	{
		public Controller button;
		public SexGroup sexGroup;
		public GTextField name;
		public GButton create_role;

		public const string URL = "ui://3aca1uguktwie";

		public static CreateRole CreateInstance()
		{
			return (CreateRole)UIPackage.CreateObject("Login","CreateRole");
		}

		public CreateRole()
		{
		}

		public override void ConstructFromXML(XML xml)
		{
			base.ConstructFromXML(xml);

			button = this.GetController("button");
			sexGroup = (SexGroup)this.GetChild("sexGroup");
			name = (GTextField)this.GetChild("name");
			create_role = (GButton)this.GetChild("create_role");
		}
	}
}
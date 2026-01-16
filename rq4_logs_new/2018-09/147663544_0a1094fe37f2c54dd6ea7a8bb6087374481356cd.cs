using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace Frindr
{
	[XamlCompilation(XamlCompilationOptions.Compile)]
	public partial class Login : ContentPage
	{
        string userRecords = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.Personal), "user.txt");
        public Login ()
		{
			InitializeComponent ();
		}

        public void CheckUsername()
        {
            using (StreamReader streamReader = new StreamReader(userRecords))
            {
                string content = streamReader.ReadToEnd();

                if(txtUsername.Text == content)
                {
                    Profile profile = new Profile();
                    Navigation.PushModalAsync(profile);
                }
            }
        }
	}
}
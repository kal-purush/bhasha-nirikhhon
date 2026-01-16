using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace Frindr
{
	[XamlCompilation(XamlCompilationOptions.Compile)]
	public partial class ExitPage : ContentPage
	{
		public ExitPage ()
		{
			InitializeComponent ();

            ShutDown();
        }

        private void ShutDown()
        {
            this.ShutDown();
        }
	}
}
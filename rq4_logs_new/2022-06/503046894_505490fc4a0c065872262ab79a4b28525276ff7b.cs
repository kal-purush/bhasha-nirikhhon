using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace XAMLInXamarinForms.Views.Fitness
{
    [XamlCompilation(XamlCompilationOptions.Compile)]
    public partial class FitnessView : CarouselPage
    {
        public FitnessView()
        {
            InitializeComponent();
        }
    }
}
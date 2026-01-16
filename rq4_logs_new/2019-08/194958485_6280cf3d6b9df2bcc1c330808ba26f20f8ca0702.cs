using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Taskio.Interface;
using Xamarin.Forms;
using Xamarin.Forms.Xaml;

namespace Taskio.Views
{
    [XamlCompilation(XamlCompilationOptions.Compile)]
    public partial class ImagePickerPage : ContentPage
    {
        public bool IsLoading{get;set;}
        public ImagePickerPage()
        {
            InitializeComponent();
            LoadImage();
            Spinner.SetBinding(ActivityIndicator.IsRunningProperty, nameof(Image.IsLoading));
            Spinner.SetBinding(ActivityIndicator.IsVisibleProperty, nameof(Image.IsLoading));
            Spinner.BindingContext = image;
            SpinnerContainer.SetBinding(ActivityIndicator.IsRunningProperty, nameof(Image.IsLoading));
            SpinnerContainer.SetBinding(ActivityIndicator.IsVisibleProperty, nameof(Image.IsLoading));
            SpinnerContainer.BindingContext = image;
        }
        async Task LoadImage()
        {
            Stream stream = await DependencyService.Get<IPhotoPicker>().GetImageStreamAsync();
            if (stream != null)
            {
                image.Source = ImageSource.FromStream(() => stream);
            }
        }
    }
}
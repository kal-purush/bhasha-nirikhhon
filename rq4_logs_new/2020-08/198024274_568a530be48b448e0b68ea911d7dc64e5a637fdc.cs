using Caliburn.Micro;
using Nt.Utils.ControlInterfaces;
using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;

namespace Nt.Utils.ExtensionMethods
{
    public static class IWindowManagerExtentions
    {
        public static bool? ShowNtDialog(this IWindowManager source, NtViewModelBase viewModel,NtWindowSize windowModel)
        {
            var (height, width) = windowModel switch
            {
                NtWindowSize.SmallLandscape => (300, 200 ),
                NtWindowSize.MediumLandscape => (400,300),
                _ => (500,400)
            };
            return source.ShowNtDialog(viewModel, height,width);
        }

        private static bool? ShowNtDialog(this IWindowManager source,NtViewModelBase viewModel,double width,double height)
        {

            dynamic settings = new ExpandoObject();
            settings.Height = height;
            settings.Width = width;
            settings.SizeToContent = SizeToContent.Manual;

            return source.ShowDialog(viewModel,null,settings);
        }
    }
    public enum NtWindowSize
    {
        SmallLandscape,
        MediumLandscape,
        LargeLandscape,

        SmallPortrait,
        MediumPortrait,
        LargePrtrait
    }
}
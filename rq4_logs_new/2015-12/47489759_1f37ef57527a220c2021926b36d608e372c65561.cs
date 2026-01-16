using SnapNShare.Views;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using TestingInfrastructure.STA;
using System.Windows;
using Xunit;

namespace SnapNShare.IntegrationTests
{
    public class OverlayWindowTests
    {
        [STAFact]
        public void OverlayWindowStretchesOnFullDesktop()
        {
            using (var sut = new OverlayWindow(null))
            {
                sut.Show();
                Assert.True(sut.Width == SystemParameters.VirtualScreenWidth);
                Assert.True(sut.Height == SystemParameters.VirtualScreenHeight);
                sut.Close();
            }
        }

        [STAFact]
        public void OverlayWindowImageIsNullWithoutCapture()
        {
            using (var sut = new OverlayWindow(null))
            {
                sut.Show();
                var result = sut.GetClippedImage();
                Assert.Null(result);
                sut.Close();
            }
        }
    }
}
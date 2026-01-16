using Microsoft.Phone.Controls;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls.Primitives;
using System.Windows.Media;

namespace HDV.FacebookSDK.Controls
{
    internal class Dialog
    {
        private Popup popup;
        private PhoneApplicationFrame rootFrame;
        private PhoneApplicationPage currentPage;

        private CompositeTransform compositionTransform;

        public Dialog()
        {
            this.popup = new Popup();
            this.compositionTransform = new CompositeTransform();

            this.CanDismiss = true;
        }

        /// <summary>
        /// Nội dung của dialog
        /// </summary>
        public UIElement DialogContent
        {
            set
            {
                popup.Child = value;
                if (popup.Child != null)
                {
                    popup.Child.RenderTransformOrigin = new Point(0.5, 0.5);
                    popup.Child.RenderTransform = compositionTransform;
                }
            }
            get
            {
                return popup.Child;
            }
        }

        public bool CanDismiss
        {
            set;
            get;
        }

        public bool IsShowing
        {
            get
            {
                return popup.IsOpen;
            }
        }

        public void Show()
        {
            //Get Application Frame
            this.rootFrame = Application.Current.RootVisual as PhoneApplicationFrame;
            if (rootFrame == null)
                return;

            //Get Application Page
            this.currentPage = rootFrame.Content as PhoneApplicationPage;
            if (currentPage == null)
                return;

            //Handle System Event
            rootFrame.BackKeyPress += OnKeyBackPress;
            currentPage.OrientationChanged += OnOrientationChanged;

            ChangeOrientation(currentPage.Orientation);

            //Show dialog
            popup.IsOpen = true;

            //Raise Event
            if (Showed != null)
                Showed(this, null);
        }

        private void OnOrientationChanged(object sender, OrientationChangedEventArgs e)
        {
            ChangeOrientation(e.Orientation);
        }

        private void ChangeOrientation(PageOrientation orientation)
        {
            double screenWidth = Application.Current.Host.Content.ActualWidth;
            double screenHeight = Application.Current.Host.Content.ActualHeight;

            double dialogWidth = screenWidth;
            double dialogHeight = screenHeight;

            var dialogContent = popup.Child;

            switch (orientation)
            {
                case PageOrientation.Landscape:
                case PageOrientation.LandscapeLeft:
                    dialogWidth = screenHeight;
                    dialogHeight = screenWidth;

                    compositionTransform.Rotation = 90;
                    break;
                case PageOrientation.LandscapeRight:
                    dialogWidth = screenHeight;
                    dialogHeight = screenWidth;

                    compositionTransform.Rotation = 270;
                    break;
                case PageOrientation.Portrait:
                case PageOrientation.PortraitDown:
                case PageOrientation.PortraitUp:
                    compositionTransform.Rotation = 0;
                    break;
                    
            }

            popup.HorizontalOffset = (screenWidth - dialogWidth) / 2;
            popup.VerticalOffset = (screenHeight - dialogHeight) / 2;

            dialogContent.SetValue(FrameworkElement.WidthProperty, dialogWidth);
            dialogContent.SetValue(FrameworkElement.HeightProperty, dialogHeight);
        }

        public void Dismiss()
        {
            rootFrame.BackKeyPress -= OnKeyBackPress;
            currentPage.OrientationChanged -= OnOrientationChanged;

            popup.IsOpen = false;

            //Raise Event
            if (Dismissed != null)
                Dismissed(this, null);
        }

        private void OnKeyBackPress(object sender, System.ComponentModel.CancelEventArgs e)
        {
            if (!IsShowing)
                return;

            e.Cancel = true;

            if (CanDismiss)
                Dismiss();
        }

        public event EventHandler<EventArgs> Showed;
        public event EventHandler<EventArgs> Dismissed;
    }
}
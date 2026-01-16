using System;
using System.Drawing;
using System.Windows.Forms;

namespace Boxinator_V2 {
    public class Image {
        private string _imagePath;
        private double[][] annotationBoxes;
        
        public Image(string imagePath) {
            _imagePath = imagePath;
        }
        
        public void AddAnnotationBox(double[] box) {
            //annotationBoxes.Add(box);
        }

        public Bitmap Get() {
            Bitmap image;
            try {
                image = new Bitmap(_imagePath);
            }
            catch (Exception e) {
                MessageBox.Show("Image not found: " + _imagePath);
                image = Bitmap.FromHicon(SystemIcons.Error.Handle);
            }
            return image;
        }
    }
}
        public void SkewCorrection()
        {
            string fileName = @"D:\Skew Correction\Test2\UploadedFiles\3.jpg";
            Bitmap bitmap;
            using (Stream bmpStream = System.IO.File.Open(fileName, System.IO.FileMode.Open))
            {
                System.Drawing.Image image = System.Drawing.Image.FromStream(bmpStream);
                bitmap = new Bitmap(image);

                using (Bitmap targetBmp = bitmap.Clone(new Rectangle(0, 0, bitmap.Width, bitmap.Height), PixelFormat.Format8bppIndexed))
                {
                    DocumentSkewChecker skewChecker = new DocumentSkewChecker();
                    // get documents skew angle
                    double angle = skewChecker.GetSkewAngle(targetBmp);
                    if (angle != -90)
                    {
                        // create rotation filter
                        RotateBilinear rotationFilter = new RotateBilinear(-angle);
                        rotationFilter.FillColor = Color.White;
                        rotationFilter.KeepSize = true;
                        Bitmap rotationFilterApplicableImg = bitmap.Clone(new Rectangle(0, 0, bitmap.Width, bitmap.Height), PixelFormat.Format24bppRgb);
                        // rotate image applying the filter
                        Bitmap skewCorrectedimg = rotationFilter.Apply(rotationFilterApplicableImg);

                        skewCorrectedimg.Save(@"D:\After_VS2017\MvcTestMay2\MvcTestMay2\UploadedFiles\" + Guid.NewGuid().ToString() + ".jpg");
                    }
                }
            }
        }
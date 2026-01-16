using System;
using HalconDotNet;
using System.Drawing;
using System.Drawing.Imaging;
using System.Runtime.InteropServices;
using System.Threading.Tasks;


namespace BitmapHImageConverter
{
    public static class BitmapHImageConverter
    {
        /// <summary>
        /// Create a bitmap from a HImage. Image data is copied -> new bitmap is independent of HImage lifetime
        /// </summary>
        /// <param name="ho_Image">input HImage</param>
        /// <returns>output new System.Drawing.Imaging.Bitmap</returns>
        public static Bitmap HImage2Bitmap(HImage ho_Image)
        {
            int iWidth, iHeight, iNumChannels;
            IntPtr ip_R, ip_G, ip_B, ip_Data;
            String sType;
            // null return object
            Bitmap bitmap = null;
            try
            {
                //
                // Note that pixel data is stored differently in a System.Drawing.Bitmap:
                // a) Stride:
                // stride is the width, rounded up to a multiple of 4 (padding)
                // Size of data array HALCON: heigth*width, Bitmap: heigth*stride
                // compare: https://msdn.microsoft.com/en-us/library/zy1a2d14%28v=vs.110%29.aspx
                // b) RGB data storage:
                // Bitmap: one array, alternating red/green/blue (HALCON: three arrays)
                //
                // get the number of channels to run different conversion method
                iNumChannels = ho_Image.CountChannels();
                if (iNumChannels != 1 && iNumChannels != 3)
                    throw new Exception("Conversion of HImage to Bitmap failed. Number of channels of the HImage is: " +
                        iNumChannels + ". Conversion rule exists only for images with 1 or 3 chanels");
                if (iNumChannels == 1)
                {
                    //
                    // 1) Get the image pointer
                    ip_Data = ho_Image.GetImagePointer1(out sType, out iWidth, out iHeight);
                    //
                    // 2) Calculate the stride
                    int iStride = CalculateBitmapStride(iWidth, iNumChannels);
                    //
                    // 3) Create a new gray Bitmap object, allocating the necessary (managed) memory 
                    bitmap = new Bitmap(iWidth, iHeight, PixelFormat.Format8bppIndexed);
                    // note for high performance, image can be copied by reference (see HImage2BitmapByReference)
                    // 
                    // 4) Copy the image data directly into the bitmap data object
                    CopyBytesIntoBitmap(ref bitmap, ip_Data, iWidth, iStride);
                    //
                    // 5) Adjust color palette to grayscale (linearized grayscale)
                    bitmap.Palette = CreateGrayColorPalette(bitmap);
                }
                if (iNumChannels == 3)
                {
                    //
                    // 1) Calculate the stride
                    ho_Image.GetImagePointer3(out ip_R, out ip_G, out ip_B, out sType, out iWidth, out iHeight);
                    int iStride = CalculateBitmapStride(iWidth, iNumChannels);
                    //
                    // 2) Create interleaved image in HALCON
                    HImage ho_ImageInterleaved = ho_Image.InterleaveChannels("rgb", iStride, 0);
                    //
                    // 3) Create a new RGB Bitmap object, allocating the necessary (managed) memory 
                    bitmap = new Bitmap(iWidth, iHeight, PixelFormat.Format24bppRgb);
                    // note for high performance, image can be copied by reference (see HImage2BitmapByReference)
                    //
                    // 4) Copy bytes
                    int iWidthIntlvd, iHeightIntlvd;
                    ip_Data = ho_ImageInterleaved.GetImagePointer1(out sType, out iWidthIntlvd, out iHeightIntlvd);
                    CopyBytesIntoBitmap(ref bitmap, ip_Data, iStride, iStride);
                    //
                    // 5) Free temp HALCON image
                    ho_ImageInterleaved.Dispose();
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Conversion of HImage to Bitmap failed.", ex);
            }
            return bitmap;
        }


        /// <summary>
        /// Create a bitmap from a HALCON HImage by reference.
        /// Make sure to keep the interleaved HImage alive as long as the bitmap is used.
        /// in case of 1 channel image, padding must be 0
        /// </summary>
        /// <param name="ho_Image"></param>
        /// <returns></returns>
        public static Bitmap HImage2BitmapByReference(HImage ho_Image, out HImage ho_ImageInterleaved)
        {
            int iWidth, iHeight, iNumChannels;
            IntPtr ip_Gray;
            String sType;
            // null return objects
            Bitmap bitmap = null;
            try
            {
                ho_ImageInterleaved = new HImage();
                //
                // Note that pixel data is stored differently in System.Drawing.Bitmap
                iNumChannels = ho_Image.CountChannels();
                if (iNumChannels == 1)
                {
                    //
                    // 1) Get the image pointer
                    ip_Gray = ho_Image.GetImagePointer1(out sType, out iWidth, out iHeight);
                    //
                    // 2) Calculate the stride
                    int iPadding = CalculateBitmapPadding(iWidth, iNumChannels);
                    if (iPadding > 0)
                        throw new Exception("Conversion of HImage to Bitmap failed. " +
                            " Padding (=width modulo 4) of Bitmap not zero (mandatory to copy by reference). " +
                            "To solve, please use HImage2Bitmap");
                    //
                    // 3) Create a new gray Bitmap object, copy by reference.
                    // keep in mind that the bitmap object's validity relies on the HImage lifetime
                    bitmap = new Bitmap(iWidth, iHeight, iWidth, PixelFormat.Format8bppIndexed, ip_Gray);
                    //
                    // 4) Adjust palette to grayscale (linearized grayscale)
                    bitmap.Palette = CreateGrayColorPalette(bitmap);
                }
                else if (iNumChannels == 3)
                {
                    //
                    // 1) Get the image stride
                    ho_Image.GetImagePointer1(out sType, out iWidth, out iHeight);
                    int iStride = CalculateBitmapStride(iWidth, iNumChannels);
                    //
                    // 2) Create an interleaved HALCON image using operator interleave_channels
                    ho_ImageInterleaved = ho_Image.InterleaveChannels("rgb", iStride, 0);
                    int iWidthIntlvd, iHeightIndlvd;
                    ip_Gray = ho_ImageInterleaved.GetImagePointer1(out sType, out iWidthIntlvd, out iHeightIndlvd);
                    //
                    // 3) Create a new gray Bitmap object, copy by reference.
                    // keep in mind that the bitmap object's validity relies on the HImage lifetime
                    bitmap = new Bitmap(iWidth, iHeight, iStride, PixelFormat.Format24bppRgb, ip_Gray);
                }
                else if (iNumChannels == 4)
                    throw new NotImplementedException();
                else
                {
                    throw new Exception("Conversion of HImage to Bitmap failed. Number of channels in HImage is: " +
                           iNumChannels + ". Direct conversion by reference only possible with images of 1,3 or 4 channels.");
                }
            }
            catch (Exception ex)
            {
                throw new Exception("Conversion of HImage to Bitmap failed.", ex);
            }
            return bitmap;
        }


        /// <summary>
        /// Convert a Bitmap into HALCON HImage.
        /// Bitmap data is copied, so memory can be released afterwards.
        /// note: in case of 8Bit bitmap images, and an image width divisable by 4, one can also use operator GenImage1Extern
        /// </summary>
        /// <param name="bitmap"></param>
        /// <returns></returns>
        public static HImage Bitmap2HImage(Bitmap bitmap)
        {
            HImage ho_ImageOut;
            BitmapData bmpData;
            IntPtr pBitmap, ip_Data;
            PixelFormat pf;
            try
            {
                ho_ImageOut = new HImage();
                pf = bitmap.PixelFormat;
                int iWidth = bitmap.Width;
                int iHeight = bitmap.Height;
                // one channel image
                if (pf == PixelFormat.Format8bppIndexed)
                {
                    int iPadding = CalculateBitmapPadding(bitmap.Width, 1);
                    int iStride = CalculateBitmapStride(iWidth, 1);
                    if (iPadding == 0)
                    {
                        // Access bitmap data object
                        bmpData = bitmap.LockBits(new Rectangle(0, 0, iWidth, iHeight), ImageLockMode.ReadOnly, pf);
                        //
                        // Create new HObject
                        // Note that GenImage1 allocates new memory. Use GenImage1Extern to create HImage by reference. 
                        ho_ImageOut.GenImage1("byte", iWidth, iHeight, bmpData.Scan0);
                        bitmap.UnlockBits(bmpData);
                    }
                    // In case of padding, data must be copied manually
                    else
                    {
                        // allocate HALCON object
                        ho_ImageOut = new HImage("byte", iWidth, iHeight);
                        string sType;
                        ip_Data = ho_ImageOut.GetImagePointer1(out sType, out iWidth, out iHeight);
                        //
                        // BitmapData lets us access the data in memory
                        bmpData = bitmap.LockBits(new Rectangle(0, 0, iWidth, iHeight), ImageLockMode.ReadOnly, pf);
                        // Copy data.
                        // System.Threading.Tasks.Parallel processing requires .NET framework >= 4.0 
                        Parallel.For(0, iHeight, r =>
                        {
                            IntPtr posRead = bmpData.Scan0 + r * iStride;
                            IntPtr posWrite = ip_Data + r * iWidth;
                            // copy full line at once
                            byte[] source = new byte[iWidth];
                            Marshal.Copy(posRead, source, 0, iWidth);
                            Marshal.Copy(source, 0, posWrite, iWidth);
                        });
                        //
                        // Let the .NET memory management take over control
                        bitmap.UnlockBits(bmpData);
                    }
                }
                //
                // RGB images
                else if (pf == PixelFormat.Format24bppRgb)
                {
                    // depending on the padding, the data can be copied directly
                    int iPadding = CalculateBitmapPadding(iWidth, 3);
                    if (iPadding == 0)
                    {
                        // Access bitmap data
                        bmpData = bitmap.LockBits(new Rectangle(0, 0, iWidth, iHeight), ImageLockMode.ReadOnly, pf);
                        pBitmap = bmpData.Scan0;
                        //
                        // Create new HObject
                        ho_ImageOut.GenImageInterleaved(pBitmap, "bgr", iWidth, iHeight, 0, "byte", iWidth, iHeight, 0, 0, -1, 0);
                        //
                        // Let the .NET memory management take over control
                        bitmap.UnlockBits(bmpData);
                    }
                    else
                    {
                        // Convert 24 bit bitmap to 32 bit bitmap in order to ensure
                        // that the bit width of the image (the Stride) is divisible by four.
                        // Otherwise, one might obtain skewed conversions.
                        Bitmap bImage32;
                        bImage32 = new Bitmap(bitmap.Width, bitmap.Height, PixelFormat.Format32bppRgb);
                        Graphics g = Graphics.FromImage(bImage32);
                        g.DrawImage(bitmap, new Point(0, 0));
                        g.Dispose();
                        //
                        // Obtain the image pointer.
                        Rectangle rect = new Rectangle(0, 0, bitmap.Width, bitmap.Height);
                        bmpData = bImage32.LockBits(rect, ImageLockMode.ReadOnly, PixelFormat.Format32bppRgb);
                        pBitmap = bmpData.Scan0;
                        ho_ImageOut.GenImageInterleaved(pBitmap, "bgrx", bitmap.Width, bitmap.Height, -1, "byte", bitmap.Width, bitmap.Height, 0, 0, -1, 0);
                        // Don't forget to unlock the bits again. ;-)
                        bImage32.UnlockBits(bmpData);
                        // Release memory by dereferencing and garbage collection
                        bImage32.Dispose();
                    }
                }
                else
                    throw new NotImplementedException("Method \"Bitmap2HImage\" only implemented for 1 and 3 channel images");
            }
            catch (Exception ex)
            {
                throw new Exception("Conversion of Bitmap to HImage failed.", ex);
            }
            return ho_ImageOut;
        }


        /// <summary>
        /// convert a HALCON HRegion into a monochrome System.Drawing.Bitmap
        /// </summary>
        /// <param name="ho_Region"></param>
        /// <returns></returns>
        static public Bitmap HRegion2Bitmap(HRegion ho_Region, int iWidth, int iHeight)
        {
            // null return object
            Bitmap bitmap = null;
            try
            {
                // get region points
                HTuple rows, cols;
                ho_Region.GetRegionPoints(out rows, out cols);
                int iPoints = rows.Length;
                // return if region contains no points
                if (iPoints == 0)
                    return bitmap;
                //
                // create a new monochrome Bitmap object, allocating the necessary (managed) memory 
                bitmap = new Bitmap(iWidth, iHeight, PixelFormat.Format1bppIndexed);
                BitmapData bmpData = bitmap.LockBits(new Rectangle(0, 0, iWidth, iHeight), ImageLockMode.WriteOnly, bitmap.PixelFormat);
                //
                // loop: set bit value for each point in the monochrome BitmapData
                for (int i = 0; i < iPoints; i++)
                {
                    // if region point is not outside of image
                    if (rows[i] >= 0 && rows[i] < iHeight && cols[i] >= 0 && cols[i] < iWidth)
                    {
                        // pointer to BitmapData + desired offset to read/write
                        int byteNumber = (bmpData.Stride * 8 * rows[i] + cols[i]) / 8;
                        int bitNumber = cols[i] % 8;
                        bitNumber = 7 - bitNumber;                              // reverse bit order
                        byte b = Marshal.ReadByte(bmpData.Scan0, byteNumber);   // read the byte
                        byte mask = (byte)(1 << bitNumber);                     // modify byte (set desired bit to 1)
                        b = b |= mask;
                        Marshal.WriteByte(bmpData.Scan0, byteNumber, b);        // write the modified byte
                    }
                }
                // let the windows memory management take over control
                bitmap.UnlockBits(bmpData);
            }
            catch (Exception ex)
            {
                throw new Exception("Conversion of HRegion to monochrome Bitmap failed.", ex);
            }
            return bitmap;
        }


        /// <summary>
        /// Helper method to copy the (interleaved) image data into the bitmap
        /// </summary>
        /// <param name="bytesPerRow">number of bytes to copy from source per row</param>
        public static void CopyBytesIntoBitmap(ref Bitmap bitmap, IntPtr ip_Gray, int bytesPerRow, int iStride)
        {
            // Copy the image data directly into the bitmap data object
            // BitmapData lets us access the data in memory
            int iHeight = bitmap.Height;
            BitmapData bmpData = bitmap.LockBits(new Rectangle(0, 0, bitmap.Width, iHeight),
                ImageLockMode.WriteOnly, bitmap.PixelFormat);
            // System.Threading.Tasks.Parallel processing requires .NET framework >= 4.0 
            Parallel.For(0, iHeight, r =>
            {
                IntPtr posRead = ip_Gray + r * bytesPerRow;
                IntPtr posWrite = bmpData.Scan0 + r * iStride;
                // copy full line at once
                byte[] source = new byte[bytesPerRow];
                Marshal.Copy(posRead, source, 0, bytesPerRow);
                Marshal.Copy(source, 0, posWrite, bytesPerRow);
            });
            //
            // Let the .NET memory management take over control
            bitmap.UnlockBits(bmpData);
        }



        /// <summary>
        /// Helper method to create and return a color palette for grayscale bitmaps
        /// </summary>
        /// <returns></returns>
        public static ColorPalette CreateGrayColorPalette(Bitmap bitmap)
        {
            // ColorPalette has no constructor -> obtain it from the Bitmap
            ColorPalette cp_P = bitmap.Palette;
            for (int i = 0; i < 256; i++)
            {
                cp_P.Entries[i] = Color.FromArgb(i, i, i);
            }
            return cp_P;
        }

        /// <summary>
        /// Helper method to calculate the stride of a Drawing.Bitmap at given width
        /// </summary>
        /// <param name="iWidth"></param>
        /// <returns></returns>
        public static int CalculateBitmapStride(int iWidth, int iChannels)
        {
            // Calculate the stride
            // Stride = image_width + padding
            int iPadding = CalculateBitmapPadding(iWidth, iChannels);
            int iStride = iWidth * iChannels + iPadding;
            return iStride;
        }

        /// <summary>
        /// Helper method to calculate the Padding of a Drawing.Bitmap at given width
        /// </summary>
        /// <param name="iWidth"></param>
        /// <returns></returns>
        public static int CalculateBitmapPadding(int iWidth, int iChannels)
        {
            // Calculate the padding
            // padding: image width modulo 4
            int iPadding = (4 - (iWidth * iChannels % 4)) % 4;
            return iPadding;
        }
    }
}
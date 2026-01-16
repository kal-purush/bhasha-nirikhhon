using System;
using System.Collections;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.Runtime.InteropServices;

namespace IHGVM_VideoManeuver
{
    public abstract class AviRepel
    {
        protected int aviFile = 0;
        protected IntPtr aviStream;
        protected ArrayList streams = new ArrayList();

        internal int FilePointer
        {
            get { return aviFile; }
        }

        internal virtual IntPtr StreamPointer
        {
            get { return aviStream; }
        }        
    }

    public class VideoRepel : AviRepel
    {
        public VideoRepel(string fileName, double frameRate, Bitmap firstFrame)
        {
            this.frameRate = frameRate;
            Avi32.AVIFileInit();
            int result = Avi32.AVIFileOpen(ref aviFile, fileName, AVI32_CONSTANTS.CONS_WRITE | AVI32_CONSTANTS.CONS_CREATE, 0);
            if (result != 0)
            {

            }

            Initialize(aviFile, frameRate, firstFrame);            
            
        }

        #region Variables & Properties

        private int getFrameObject;
        private int frameSize;
        protected double frameRate;
        private int width;
        private int height;
        private Int16 countBitsPerPixel;
        protected int countFrames = 0;
        protected int firstFrame = 0;

        public int FrameSize { get { return frameSize; } }
        public double FrameRate { get { return frameRate; } }
        public int Width { get { return width; } }
        public int Height { get { return height; } }
        public Int16 CountBitsPerPixel { get { return countBitsPerPixel; } }
        public int CountFrames { get { return countFrames; } }
        public int FirstFrame { get { return firstFrame; } }

        #endregion

        private void Initialize(int aviFile, double frameRate, Bitmap firstFrame)
        {
            this.aviFile = aviFile;
            this.frameRate = frameRate;
            this.firstFrame = 0;

            BitmapData bmpData = firstFrame.LockBits(new Rectangle(0, 0, firstFrame.Width, firstFrame.Height), ImageLockMode.ReadOnly, firstFrame.PixelFormat);

            this.frameSize = bmpData.Stride * bmpData.Height;
            this.width = firstFrame.Width;
            this.height = firstFrame.Height;
            this.countBitsPerPixel = ConvertPixelFormatToBitCount(firstFrame.PixelFormat);

            firstFrame.UnlockBits(bmpData);
            CreateStream();
            AddFrame(firstFrame);
        }

        private void CreateStream()
        {
            CreateStreamWithoutFormat();
            SetFormat(aviStream);
        }

        private void CreateStreamWithoutFormat()
        {
            int scale = 1;
            double rate = frameRate;
            GetRateAndScale(ref rate, ref scale);

            Avi32.AVISTREAMINFO strhdr = new Avi32.AVISTREAMINFO();
            strhdr.fccType = Avi32.mmioStringToFOURCC("vids", 0);
            strhdr.fccHandler = Avi32.mmioStringToFOURCC("CVID", 0);
            strhdr.dwFlags = 0;
            strhdr.dwCaps = 0;
            strhdr.wPriority = 0;
            strhdr.wLanguage = 0;
            strhdr.dwScale = (int)scale;
            strhdr.dwRate = (int)rate;
            strhdr.dwStart = 0;
            strhdr.dwLength = 0;
            strhdr.dwInitialFrames = 0;
            strhdr.dwSuggestedBufferSize = frameSize;
            strhdr.dwQuality = -1;
            strhdr.dwSampleSize = 0;
            strhdr.rcFrame.top = 0;
            strhdr.rcFrame.left = 0;
            strhdr.rcFrame.bottom = (uint)height;
            strhdr.rcFrame.right = (uint)width;
            strhdr.dwEditCount = 0;
            strhdr.dwFormatChangeCount = 0;
            strhdr.szName = new UInt16[64];

            int result = Avi32.AVIFileCreateStream(aviFile, out aviStream, ref strhdr);
            if (result != 0)
            {
                throw new Exception("Exception in AVIFileCreateStream: " + result.ToString());
            }
        }

        private Int16 ConvertPixelFormatToBitCount(PixelFormat format)
        {
            String formatName = format.ToString();

            formatName = formatName.Substring(6, 2);
            Int16 bitCount = 0;
            if (Char.IsNumber(formatName[1]))
                bitCount = Int16.Parse(formatName);
            else
                bitCount = Int16.Parse(formatName[0].ToString());

            return bitCount;
        }

        private void GetRateAndScale(ref double frameRate, ref int scale)
        {
            scale = 1;
            while (frameRate != (long)frameRate)
            {
                frameRate = frameRate * 10;
                scale *= 10;
            }
        }

        private void SetFormat(IntPtr aviStream)
        {
            Avi32.BITMAPINFOHEADER bi = new Avi32.BITMAPINFOHEADER();
            bi.biSize = Marshal.SizeOf(bi);
            bi.biWidth = width;
            bi.biHeight = height;
            bi.biPlanes = 1;
            bi.biBitCount = countBitsPerPixel;
            bi.biSizeImage = frameSize;

            int result = Avi32.AVIStreamSetFormat(aviStream, 0, ref bi, bi.biSize);
            if (result != 0) { throw new Exception("Error in VideoStreamSetFormat: " + result.ToString()); }
        }

        public void AddFrame(Bitmap bmp)
        {
            bmp.RotateFlip(RotateFlipType.RotateNoneFlipY);
            BitmapData bmpDat = bmp.LockBits(new Rectangle(0, 0, bmp.Width, bmp.Height), ImageLockMode.ReadOnly, bmp.PixelFormat);
            int result = Avi32.AVIStreamWrite(StreamPointer, countFrames, 1, bmpDat.Scan0, (Int32)(bmpDat.Stride * bmpDat.Height), 0, 0, 0);

            if (result != 0)
            {
                throw new Exception("Exception in VideoStreamWrite: " + result.ToString());
            }

            bmp.UnlockBits(bmpDat);
            countFrames++;
        }

        public void Close()
        {
            Avi32.AVIStreamRelease(StreamPointer);
            Avi32.AVIFileRelease(aviFile);
            Avi32.AVIFileExit();
        }

    }
}
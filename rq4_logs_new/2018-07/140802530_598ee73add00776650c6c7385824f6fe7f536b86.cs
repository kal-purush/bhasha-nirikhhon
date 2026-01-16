using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.Text;
using Emgu.CV;
using Emgu.CV.Structure;
namespace Hs.Utility.OpenCv
{
    public static class ImageHelper
    {
        ///// <summary>
        ///// 图片框人脸
        ///// </summary>
        ///// <param name="image"></param>
        ///// <param name="width"></param>
        ///// <param name="height"></param>
        ///// <param name="lineWidth"></param>
        //public static List<Rectangle> CheckFaces(ref Bitmap image)
        //{
        //    var imgWidth = image.Width;
        //    var imgHeight = image.Height;

        //    var facerect = GetImageFaces(image, new System.Drawing.Size(130, 130)).Select(x => ConvertFaceRect(x, imgWidth, imgHeight)).ToList();
        //    return facerect;
        //}
        ///// <summary>
        ///// 图片框人脸
        ///// </summary>
        ///// <param name="image"></param>
        ///// <param name="width"></param>
        ///// <param name="height"></param>
        ///// <param name="lineWidth"></param>
        //public static List<Rectangle> DrawFaces(ref Bitmap image, int width, int height, int lineWidth)
        //{
        //    var facerect = CheckFaces(ref image);
        //    DrawFacesRect(ref image, facerect, width, height, lineWidth);
        //    return facerect;
        //}
        #region **人脸检测

        //人脸检测器
        private static CascadeClassifier faceClassifier;

        /// <summary>
        /// 获取人脸框
        /// </summary>
        /// <param name="img">要获取人脸的相片</param>
        /// <param name="size">最小识别尺寸，动态建议70*70，静态建议130*130</param>
        /// <returns></returns>
        public static List<Rectangle> GetImageFaces(Bitmap img, System.Drawing.Size size)
        {
            if (faceClassifier == null)
                faceClassifier = new CascadeClassifier(@"cascades/haarcascade_frontalface_default.xml");
            //faceClassifier = new CascadeClassifier(@"cascades/lbpcascade_frontalface.xml");
            List<Rectangle> resultFaceImgInfos = new List<Rectangle>();

            if (img != null)
            {
                //取图片变化的倍率 1920*1080
                double imgRate = img.Width * 1.0 / 1920 > img.Height * 1.0 / 1080 ? 1920 * 1.0 / img.Width : 1080 * 1.0 / img.Height;
                //转换图片最小框选值
                int width = Convert.ToInt32(size.Width / imgRate);
                int height = Convert.ToInt32(size.Height / imgRate);
                width = width < 10 ? 10 : width;
                height = height < 10 ? 10 : height;
                System.Drawing.Size newSize = new System.Drawing.Size(width, height);
                using (Image<Gray, byte> bgrframe = new Image<Gray, byte>(img))
                {
                    resultFaceImgInfos.AddRange(faceClassifier.DetectMultiScale(bgrframe, 1.15, 8, newSize));
                }
            }
            return resultFaceImgInfos;
        }

        /// <summary>
        /// 转换人脸框变成框选整个头部
        /// </summary>
        /// <param name="width">图片宽度</param>
        /// <param name="height">图片高度</param>
        /// <returns></returns>
        public static Rectangle ConvertFaceRect(Rectangle rect, int width, int height)
        {
            //计算高
            int y = Convert.ToInt32(rect.Y - rect.Height * 0.5 / 1.5);
            y = y > 0 ? y : 0;
            int h = Convert.ToInt32(rect.Height *2);
            h = y + h > height ? height - y : h;

            //计算宽
            int x = Convert.ToInt32(rect.X - rect.Width * 0.5 / 2);
            x = x > 0 ? x : 0;
            int w = Convert.ToInt32(rect.Width * 1.6);
            w = x + w > width ? width - x : w;

            Rectangle resultRect = new Rectangle(x, y, w, h);
            return resultRect;
        }

        /// <summary>
        /// 转换人脸框变成框选整个头部
        /// </summary>
        /// <returns></returns>
        public static List<Rectangle> ConvertFaceRect(List<Rectangle> rectList, int width, int height)
        {
            List<Rectangle> resultRects = new List<Rectangle>();
            foreach (var rect in rectList)
            {
                resultRects.Add(ConvertFaceRect(rect, width, height));
            }
            return resultRects;
        }
        /// <summary>
        /// 按比例缩放图片（不留黑边）
        /// </summary>
        /// <param name="image"></param>
        /// <param name="wndWidth"></param>
        /// <param name="wndHeigth"></param>
        /// <returns></returns>
        public static Bitmap BitmapResize2(Bitmap image, int width, int height)
        {
            int nSourceWidth = image.Width;
            int nSourceHeigth = image.Height;

            float fDestWidth, fDestHight;

            if (nSourceWidth < width && nSourceHeigth < height)
            {
                fDestWidth = nSourceWidth;
                fDestHight = nSourceHeigth;
            }


            float fRatio = (float)(nSourceHeigth * 1.0 / nSourceWidth);
            if (fRatio * width < height)
            {
                fDestWidth = (float)width;
                fDestHight = fDestWidth * fRatio;
            }
            else
            {
                fDestHight = (float)height;
                fDestWidth = fDestHight / fRatio;
            }

            int sW = (int)fDestWidth;
            int sH = (int)fDestHight;

            Bitmap outBmp = new Bitmap(sW, sH);
            Graphics g = Graphics.FromImage(outBmp);
            g.Clear(Color.Transparent);
            // 设置画布的描绘质量         
            g.CompositingQuality = CompositingQuality.HighQuality;
            g.SmoothingMode = SmoothingMode.HighQuality;
            g.InterpolationMode = InterpolationMode.HighQualityBicubic;
            g.DrawImage(image, new Rectangle(0, 0, sW, sH), 0, 0, image.Width, image.Height, GraphicsUnit.Pixel);
            g.Dispose();
            // 以下代码为保存图片时，设置压缩质量     
            EncoderParameters encoderParams = new EncoderParameters();
            long[] quality = new long[1];
            quality[0] = 100;
            EncoderParameter encoderParam = new EncoderParameter(System.Drawing.Imaging.Encoder.Quality, quality);
            encoderParams.Param[0] = encoderParam;
            image.Dispose();

            return outBmp;
        }

        /// <summary>
        /// 按实际宽度绘制人脸线框
        /// </summary>
        /// <param name="img"></param>
        /// <param name="rectList"></param>
        /// <param name="width">图片显示控件宽度</param>
        /// <param name="height">图片显示控件高度</param>
        /// <param name="lineWidth">相对线宽</param>
        /// <returns></returns>
        public static void DrawFacesRect(ref Bitmap img, List<Rectangle> rectList, int width, int height, int lineWidth)
        {
            //取图片变化的倍率
            double imgRate = img.Width * 1.0 / width > img.Height * 1.0 / height ? width * 1.0 / img.Width : height * 1.0 / img.Height;
            int imgLineWidth = Convert.ToInt32(lineWidth / imgRate);//按控件大小与图片比例计算线宽
            if (imgLineWidth < 1)
                imgLineWidth = 1;
            if (rectList != null && rectList.Count > 0)
            {
                Graphics g = Graphics.FromImage(img);
                Pen pen = new Pen(Color.Red, imgLineWidth);
                foreach (var rect in rectList)
                {
                    g.DrawRectangle(pen, rect);
                }
                pen.Dispose();
                g.Dispose();
            }
        }

        /// <summary>
        /// 按实际宽度绘制人脸线框
        /// </summary>
        /// <param name="img"></param>
        /// <param name="rectList"></param>
        /// <param name="width">图片显示控件宽度</param>
        /// <param name="height">图片显示控件高度</param>
        /// <param name="lineWidth">相对线宽</param>
        /// <returns></returns>
        public static void DrawFacesRect(ref Bitmap img, Rectangle rect, int width, int height, int lineWidth)
        {
            //取图片变化的倍率
            double imgRate = img.Width * 1.0 / width > img.Height * 1.0 / height ? width * 1.0 / img.Width : height * 1.0 / img.Height;
            int imgLineWidth = Convert.ToInt32(lineWidth / imgRate);//按控件大小与图片比例计算线宽
            if (imgLineWidth < 1)
                imgLineWidth = 1;
            if (rect != null)
            {
                Graphics g = Graphics.FromImage(img);
                Pen pen = new Pen(Color.Red, imgLineWidth);
                g.DrawRectangle(pen, rect);
                g.Dispose();
            }
        }

        /// <summary>
        /// 裁剪图片
        /// </summary>
        /// <returns></returns>
        public static List<Bitmap> CutFacesRect(Bitmap img, List<Rectangle> rectList)
        {
            List<Bitmap> resultImgs = new List<Bitmap>();
            foreach (var rect in rectList)
            {
                Bitmap newImg = CutFacesRect(img, rect);
                resultImgs.Add(newImg);
            }
            return resultImgs;
        }

        /// <summary>
        /// 裁剪图片
        /// </summary>
        /// <returns></returns>
        public static Bitmap CutFacesRect(Bitmap img, Rectangle rect)
        {
            Bitmap resultImg = new Bitmap(rect.Width, rect.Height);
            Graphics g = Graphics.FromImage(resultImg);
            g.DrawImage(img, new Rectangle(0, 0, rect.Width, rect.Height), rect.X, rect.Y, rect.Width, rect.Height, GraphicsUnit.Pixel);
            g.Dispose();
            return resultImg;
        }

        /// <summary>
        /// 获取最大人脸框
        /// </summary>
        /// <returns></returns>
        public static Rectangle GetMaxFaceRect(List<Rectangle> rectList)
        {
            Rectangle resultRect = new Rectangle();
            if (rectList != null && rectList.Count > 0)
            {
                resultRect = rectList[0];
                foreach (var rect in rectList)
                {
                    if (resultRect.Width * resultRect.Height < rect.Width * rect.Height)
                        resultRect = rect;
                }
            }
            return resultRect;
        }





        #endregion
    }
}
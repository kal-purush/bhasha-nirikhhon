using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.IO;

namespace ComicBoxApi.App.Imaging
{
    public interface IImageService
    {
        byte[] ScaleAsThumbnail(byte[] bytes);

        byte[] TakePageSide(byte[] bytes, ImageSide imageSide);
    }

    public class ImageService : IImageService
    {
        public byte[] ScaleAsThumbnail(byte[] bytes)
        {
            using (MemoryStream image = new MemoryStream(bytes))
            {
                image.Position = 0;
                using (var fullBitmap = Bitmap.FromStream(image))
                {
                    int sourceWidth = fullBitmap.Width;
                    int sourceHeight = fullBitmap.Height;
                    int sourceX = 0;
                    int sourceY = 0;

                    int destX = 0;
                    int destY = 0;

                    float dpi = 72.0f;
                    float ratio = 2.54f / dpi;
                    int maxside = (int)(5.2f / ratio);

                    int destHeight = maxside;
                    int destWidth = destHeight * sourceWidth / sourceHeight;

                    return ApplySizeAndDimenssions(fullBitmap, sourceWidth, sourceHeight, sourceX, sourceY, destWidth, destHeight, destX, destY, dpi);
                }
            }
        }

        private static byte[] ApplySizeAndDimenssions(Image fullBitmap, int sourceWidth, int sourceHeight, int sourceX, int sourceY, int destWidth, int destHeight, int destX, int destY, float dpi)
        {
            byte[] result;
            using (Bitmap thumbnailBitmap = new Bitmap(destWidth, destHeight, PixelFormat.Format16bppRgb565))
            {
                thumbnailBitmap.SetResolution(dpi, dpi);

                using (var graphics = Graphics.FromImage(thumbnailBitmap))
                {
                    graphics.SmoothingMode = SmoothingMode.HighQuality;
                    graphics.InterpolationMode = InterpolationMode.HighQualityBicubic;
                    graphics.PixelOffsetMode = PixelOffsetMode.HighQuality;

                    graphics.DrawImage(fullBitmap,
                        new Rectangle(destX, destY, destWidth, destHeight),
                        new Rectangle(sourceX, sourceY, sourceWidth, sourceHeight),
                        GraphicsUnit.Pixel);

                    result = thumbnailBitmap.ToByteArray();
                }
            }

            return result;
        }

        public byte[] TakePageSide(byte[] bytes, ImageSide imageSide)
        {
            if (imageSide == ImageSide.Both)
            {
                return bytes;
            }
            
            using (MemoryStream image = new MemoryStream(bytes))
            {
                image.Position = 0;
                using (var fullBitmap = Bitmap.FromStream(image))
                {
                    int sourceWidth = fullBitmap.Width / 2;
                    int sourceHeight = fullBitmap.Height;
                    int sourceX = imageSide == ImageSide.Left ? 0 : sourceWidth;
                    int sourceY = 0;

                    int destX = 0;
                    int destY = 0;
                    int destWidth = sourceWidth;
                    int destHeight = sourceHeight;

                    return ApplySizeAndDimenssions(fullBitmap, sourceWidth, sourceHeight, sourceX, sourceY, destWidth, destHeight, destX, destY, fullBitmap.HorizontalResolution);
                }
            }
        }
    }
}
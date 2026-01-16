using System;
using System.Drawing;
using System.Drawing.Drawing2D;
using System.Drawing.Imaging;
using System.IO;
using System.Net;
using System.Text.RegularExpressions;
using System.Xml;

namespace MovieListCreator {
  internal static class ImageHelper {
    /// <summary>
    /// 
    /// </summary>
    /// <param name="movieListXml"></param>
    /// <param name="sOutPath"></param>
    internal static void cachePosterImages(XmlDocument outXml, string htmlFolder) {
      const string POSTER_ROOT = @"http://image.tmdb.org/t/p/original";
      const int NEW_WIDTH = 420, NEW_HEIGHT = 420;

      if (!Directory.Exists(htmlFolder + "img")) {
        Directory.CreateDirectory(htmlFolder + "img");
      }

      foreach (XmlNode ndMovie in outXml.SelectNodes("//movie[@posterpath]")) {

        XmlElement elMovie = (XmlElement)ndMovie;

        string sPosterName = elMovie.GetAttribute("posterpath");
        if (!string.IsNullOrWhiteSpace(sPosterName)) {
          string sPosterPath = POSTER_ROOT + sPosterName;
          sPosterName = sPosterName.Substring(1);
          string sFullPosterPath = Path.Combine(htmlFolder + "img", sPosterName);

          if (!File.Exists(sFullPosterPath)) {
            using (WebClient webclient = new WebClient()) {
              Byte[] ImageInBytes = webclient.DownloadData(sPosterPath);
              using (MemoryStream memoryStream = new MemoryStream(ImageInBytes)) {
                using (Image image = Image.FromStream(memoryStream)) {
                  Size size = GetNewSize(image, new Size(NEW_WIDTH, NEW_HEIGHT), true);
                  using (Image resized = new Bitmap(size.Width, size.Height)) {
                    ResizeImage(resized, image, size);
                    using (EncoderParameters myEncoderParameters = new EncoderParameters(1)) {
                      EncoderParameter myEncoderParameter = new EncoderParameter(Encoder.Quality, 50L);
                      myEncoderParameters.Param[0] = myEncoderParameter;

                      resized.Save(sFullPosterPath, GetEncoder(ImageFormat.Jpeg), myEncoderParameters);
                    }
                  }
                }
              }
            }
          }

          elMovie.SetAttribute("fullposterpath", @"img/" + sPosterName);
        }
      }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="newImage"></param>
    /// <param name="originalImage"></param>
    /// <param name="size"></param>
    public static void ResizeImage(Image newImage, Image originalImage, Size size) {
      using (Graphics graphicsHandle = Graphics.FromImage(newImage)) {
        graphicsHandle.InterpolationMode = InterpolationMode.HighQualityBicubic;
        graphicsHandle.DrawImage(originalImage, 0, 0, size.Width, size.Height);
      }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="originalImage"></param>
    /// <param name="size"></param>
    /// <param name="preserveAspectRatio"></param>
    /// <returns></returns>
    private static Size GetNewSize(Image originalImage, Size size, bool preserveAspectRatio) {

      int newWidth, newHeight;

      if (preserveAspectRatio) {
        int originalWidth = originalImage.Width;
        int originalHeight = originalImage.Height;
        float percentWidth = (float)size.Width / (float)originalWidth;
        float percentHeight = (float)size.Height / (float)originalHeight;
        float percent = percentHeight < percentWidth ? percentHeight : percentWidth;
        newWidth = (int)(originalWidth * percent);
        newHeight = (int)(originalHeight * percent);
      }
      else {
        newWidth = size.Width;
        newHeight = size.Height;
      }
      return new Size(newWidth, newHeight);
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="format"></param>
    /// <returns></returns>
    internal static ImageCodecInfo GetEncoder(ImageFormat format) {
      ImageCodecInfo[] codecs = ImageCodecInfo.GetImageDecoders();

      foreach (ImageCodecInfo codec in codecs) {
        if (codec.FormatID == format.Guid) {
          return codec;
        }
      }
      return null;
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="CategoryName"></param>
    internal static void createCategoryImages(XmlDocument xmlOut, string htmlFolder) {
      ImageFormat imgfmt = ImageFormat.Png;

      if (!Directory.Exists(htmlFolder + "img")) {
        Directory.CreateDirectory(htmlFolder + "img");
      }

      foreach (XmlNode ndCategory in xmlOut.SelectNodes("//category")) {
        XmlElement elCategory = (XmlElement)ndCategory;

        int movieCount = elCategory.SelectNodes("movie").Count;

        string sCategoryName = elCategory.GetAttribute("name") + ".png";

        string regex = String.Format("[{0}+ ]", Regex.Escape(new string(Path.GetInvalidFileNameChars())));
        Regex removeInvalidChars = new Regex(regex, RegexOptions.Singleline | RegexOptions.Compiled | RegexOptions.CultureInvariant);
        sCategoryName = removeInvalidChars.Replace(sCategoryName, "");

        string sCategoryImageName = Path.Combine(htmlFolder + "img", sCategoryName);

        using (FontFamily arialFontFamily = new FontFamily("Arial")) {
          using (Font arialFont = new Font(arialFontFamily, 22.25F, FontStyle.Bold)) {
            using (Bitmap bmpNowPlaying = new Bitmap(MovieListCreator.Resources.nowplaying, 280, 420)) {
              using (Bitmap bmpCategoryPre = Overlay.TextOverlay(bmpNowPlaying, elCategory.GetAttribute("name") + "\n\n\n\n\n\n\n\n",
                  arialFont, Color.DarkSlateGray, false, false, ContentAlignment.TopCenter, 0.8F)) {

                using (Bitmap bmpCategory = Overlay.TextOverlay(bmpCategoryPre, "\n\n\n\n\n\n\n\n(" + movieCount.ToString() + " movies)",
                    arialFont, Color.DarkSlateGray, false, false, ContentAlignment.BottomCenter, 0.4F)) {

                  bmpCategory.MakeTransparent(Color.White);

                  bmpCategory.Save(sCategoryImageName, imgfmt);
                }
              }
            }
          }
        }
        XmlElement elMovie = (XmlElement)ndCategory.AppendChild(xmlOut.CreateElement("movie"));

        elMovie.SetAttribute("name", "");
        elMovie.SetAttribute("fullposterpath", @"img/" + sCategoryName);
      }
    }
  }
}
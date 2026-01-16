using System;
using System.ComponentModel;
using System.Collections.Generic;

using Emgu.CV;
using Emgu.CV.UI;
using Emgu.CV.Structure;

namespace FeatureTesting
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			// original source image as grayscale
			Image<Gray, Byte> m_SourceImage = null;

			// raw corner strength image (must be 32-bit float)
			Image<Gray, float> m_CornerImage = null;

			// inverted thresholded corner strengths (for display)
			Image<Gray, Byte> m_ThresholdImage = null;

			m_SourceImage = new Image<Gray, byte> ("/home/tuan/MyProject/iPlantUK_Repo/RootNavLinux/RootNavLinux/bin/Debug/FeatureMapInMain.png");

			m_CornerImage = new Image<Gray, float> (m_SourceImage.Size);

			CvInvoke.CornerHarris (m_SourceImage, m_CornerImage, 3, 3, 0.01);

			m_ThresholdImage = new Image<Gray, byte> (m_SourceImage.Size);

			CvInvoke.Threshold (m_CornerImage, m_ThresholdImage, 0.0001, 255.0, Emgu.CV.CvEnum.ThresholdType.BinaryInv);
			const float MAX_INTENSITY = 255;
			int countCorner = 0;
			for (int row = 0; row < m_ThresholdImage.Rows; row++) {
				for (int col = 0; col < m_ThresholdImage.Cols; col++) {
					Gray intenP = m_ThresholdImage [row, col];

					if (intenP.Intensity >= MAX_INTENSITY) {
						//CvInvoke.Circle (m_CornerImage, new System.Drawing.Point (col, row), 5, new MCvScalar (0), 2, (Emgu.CV.CvEnum.LineType) 8, 0);

						countCorner++;

					}
				}
			}

			System.Console.WriteLine ("Total corners: " + countCorner.ToString ());
			DisplayImage (m_ThresholdImage.Mat);
			//DisplayImage (m_CornerImage.Mat);


		}

		public static void DisplayImage(Mat img, string title = "Testing", bool isFinal = true)
		{
			CvInvoke.NamedWindow(title, Emgu.CV.CvEnum.NamedWindowType.AutoSize);
			CvInvoke.Imshow(title, img);

			if (isFinal) {

				CvInvoke.WaitKey (0);
			}
		}
	}
}
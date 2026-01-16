using DigitalWatermarking.fourier;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Drawing.Imaging;
using System.Linq;
using System.Text;

namespace DigitalWatermarking.tools
{
    public class ToolBox
    {
        private static bool _useAvg = true;

        public static int[,] NormalizeMatrix(float[,] input, int newMin, int newMax)
        {
            float? oldMin = null;
            float? oldMax = null;
            int[,] output = new int[input.GetLength(0), input.GetLength(1)]; //[Row,Column]

            for (int i = 0; i < input.GetLength(0); i++)
            {
                for (int j = 0; j < input.GetLength(1); j++)
                {
                    if(oldMax.HasValue && oldMax.HasValue)
                    {
                        if(input[i,j] > oldMax.Value)
                        {
                            oldMax = input[i, j];
                        }
                        if(input[i,j] < oldMin.Value)
                        {
                            oldMin = input[i, j];
                        }
                    } else
                    {
                        oldMax = input[i, j];
                        oldMin = input[i, j];
                    }
                }
            }

            for (int i = 0; i < input.GetLength(0); i++)
            {
                for (int j = 0; j < input.GetLength(1); j++)
                {
                    output[i, j] = (int) NormalizeValue(input[i, j], oldMin.Value, oldMax.Value, newMin, newMax);
                }
            }

            return output;
        }

        public static float NormalizeValue(float value, float oldMin, float oldMax, int newMin, int newMax)
        {
            return (value - oldMin) / (oldMax - oldMin) * (newMax - newMin) + newMin;
        }

        public static Bitmap GetImage(int[,] image, int offset)
        {
            int i, j;
            Bitmap output = new Bitmap(image.GetLength(0), image.GetLength(1));
            BitmapData bitmapData1 = output.LockBits(new Rectangle(0, 0, image.GetLength(0), image.GetLength(1)),
                ImageLockMode.ReadOnly, PixelFormat.Format32bppArgb);
            unsafe
            {
                byte* imagePointer1 = (byte*)bitmapData1.Scan0;
                float sum = 0;
                for (i = 0; i < bitmapData1.Width; i++)
                {
                    for (j = 0; j < bitmapData1.Height; j++)
                    {
                        sum += image[i, j];
                    }
                }
                var avg = sum / (bitmapData1.Width * bitmapData1.Height);
                for (i = 0; i < bitmapData1.Width; i++)
                {
                    for (j = 0; j < bitmapData1.Height; j++)
                    {
                        int pos = (offset + image[i, j]) / 255;
                        if (pos == 0)
                        {
                            if (image[i, j] < avg * 2)
                            {
                                imagePointer1[2] = 255;
                                imagePointer1[1] = 255;
                                imagePointer1[0] = 255;

                            }
                            else
                            {
                                imagePointer1[2] = (byte)(255 - (image[i, j] % 255));
                                imagePointer1[1] = 0;
                                imagePointer1[0] = 255;
                            }
                            if (!_useAvg)
                            {
                                imagePointer1[2] = (byte)(255 - (image[i, j] % 255));
                                imagePointer1[1] = 0;
                                imagePointer1[0] = 255;
                            }

                        } else if(pos == 1)
                        {
                            imagePointer1[2] = 0;
                            imagePointer1[1] = (byte)(image[i, j] % 255);
                            imagePointer1[0] = 255; 
                        } else if (pos == 2)
                        {
                            imagePointer1[2] = 0;
                            imagePointer1[1] = 255;
                            imagePointer1[0] = (byte)(255 - (image[i, j] % 255));
                        } else if(pos == 3)
                        {
                            imagePointer1[2] = (byte)(255 - (image[i, j] % 255));
                            imagePointer1[1] = 255;
                            imagePointer1[0] = 0;
                        } else if(pos == 4)
                        {
                            imagePointer1[2] = 255;
                            imagePointer1[1] = (byte)(255 - (image[i, j] % 255));
                            imagePointer1[0] = 0;
                        }
                        imagePointer1[3] = 255;
                        //4 bytes per pixel
                        imagePointer1 += 4;
                    } //end for j

                    //4 bytes per pixel
                    imagePointer1 += (bitmapData1.Stride - (bitmapData1.Width * 4));
                } //end for i
            } //end unsafe

            output.UnlockBits(bitmapData1);
            return output; // col;
        }

        public static Bitmap GetImage(float[,] originalImage)
        {
            int i, j;
            Bitmap output = new Bitmap(originalImage.GetLength(0), originalImage.GetLength(1));
            BitmapData bitmapData1 = output.LockBits(new Rectangle(0, 0, originalImage.GetLength(0), originalImage.GetLength(1)),
                ImageLockMode.ReadOnly, PixelFormat.Format32bppArgb);
            int[,] image = NormalizeMatrix(originalImage, 0, 765);
            unsafe
            {
                byte* imagePointer1 = (byte*)bitmapData1.Scan0;
                for (i = 0; i < bitmapData1.Width; i++)
                {
                    for (j = 0; j < bitmapData1.Height; j++)
                    {
                        int pos = image[i, j] / 255;
                        if (pos == 0)
                        {
                            imagePointer1[2] = 0;
                            imagePointer1[1] = 255;
                            imagePointer1[0] = (byte)(255 - (image[i, j] % 255));
                        }
                        else if (pos == 1)
                        {
                            imagePointer1[2] = (byte)(255 - (image[i, j] % 255));
                            imagePointer1[1] = 255;
                            imagePointer1[0] = 0;
                        }
                        else
                        {
                            imagePointer1[2] = 255;
                            imagePointer1[1] = (byte)(255 - (image[i, j] % 255));
                            imagePointer1[0] = 0;
                        }
                        imagePointer1[3] = 255;
                        //4 bytes per pixel
                        imagePointer1 += 4;
                    } //end for j

                    //4 bytes per pixel
                    imagePointer1 += (bitmapData1.Stride - (bitmapData1.Width * 4));
                } //end for i
            } //end unsafe

            output.UnlockBits(bitmapData1);
            return output; // col;
        }

        public static float[,] GetMagnitude(COMPLEX[,] matrix)
        {
            float[,] output = new float[matrix.GetLength(0), matrix.GetLength(1)];

            for (int i = 0; i < matrix.GetLength(0); i++)
            {
                for (int j = 0; j < matrix.GetLength(1); j++)
                {
                    output[i, j] = matrix[i, j].Magnitude();
                }
            }

            return output;
        }
    }
}
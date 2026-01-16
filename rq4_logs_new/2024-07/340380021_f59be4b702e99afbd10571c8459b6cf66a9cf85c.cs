using RayTracer.MathLib;
using System;
using System.Collections.Generic;
using System.Text;

namespace RayTracer.MathLibrary.Tests;

public class CanvasTests
{
    [Theory]
    [InlineData(10, 10, 20)]
    public void CanvasConstructor_WhenCalled_CreateCanvasWithProperWidth(int expected, int width, int height)
    {
        new Canvas(width, height).Width.Should().Be(expected);
    }

    [Theory]
    [InlineData(20, 10, 20)]
    public void CanvasConstructor_WhenCalled_CreateCanvasWithProperHeight(int expected, int width, int height)
    {
        new Canvas(width, height).Height.Should().Be(expected);
    }

    [Theory]
    [InlineData(10, 20)]
    public void GetCanvas_WhenCalled_CreateAndReturnCanvasWithOnlyBlackPixels(int width, int height)
    {
        bool allBlack = true;
        Color black = new Color(0f, 0f, 0f);
        Color[,] canvas = new Canvas(width, height).GetCanvas();

        for (int i = 0; i < canvas.GetLongLength(0); i++)
        {
            for (int j = 0; j < canvas.GetLongLength(1); j++)
            {
                if (!Color.AreColorsEqual(canvas[i, j], black))
                {
                    allBlack = false;
                    break;
                }
            }
        }

        allBlack.Should().BeTrue();
    }

    [Theory]
    [InlineData(0, 0, 1f, 0f, 0f)]
    public void WritePixel_WhenCalled_ChangeColorOfOnePixel(int x, int y, double red, double green, double blue)
    {
        Color color = new Color(red, green, blue);
        Canvas canvas = new Canvas(10, 10);
        Color[,] canvasTable = canvas.GetCanvas();

        canvas.WritePixel(x, y, color);

        Color.AreColorsEqual(color, canvasTable[x, y]).Should().BeTrue();
    }

    [Theory]
    [InlineData(0f, 0f, 0f, 0, 0)]
    public void PixelAt_WhenCalled_ReturnColorOfPixel(double red, double green, double blue, int x, int y)
    {
        Color expected = new Color(red, green, blue);
        Canvas canvas = new Canvas(10, 10);
        Color[,] canvasTable = canvas.GetCanvas();

        Color.AreColorsEqual(expected, canvasTable[x, y]).Should().BeTrue();
    }

}
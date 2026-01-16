// ******************************************************************** *'
//  Copyright - Accusoft Corporation, Tampa Florida.                    *'
//  This sample code is provided to Accusoft licensees "as is"          *'
//  with no restrictions on use or modification. No warranty for        *'
//  use of this sample code is provided by Accusoft.                    *'
//                                                                      *'
//  SAMPLE PURPOSE                                                      *'
//                                                                      *'
//  This sample illustrates how to add an image to a PDF document.      *'
//                                                                      *'
//                                                                      *'
//  ARGUMENTS                                                           *'
//                                                                      *'
//   First:              The path to source image file.                 *'
//   Second (optional):  The path to output PDF file                    *'
//                       (default is <input_file_path>.pdf).            *'
//   Third (optional):   Horizontal location of image                   *'
//                       (default is 0).                                *'
//   Fourth (optional):  Vertical location of image                     *'
//                       (default is 0).                                *'
//   Fifth (optional):   Resulting image width                          *'
//                       (default is 0 - original width).               *'
//   Sixth (optional):   Resulting image height                         *'
//                       (default is 0 - original height).              *'
//   Seventh (optional): Resulting image compression type               *'
//                       (default is 0 - no compression).               *'
//   Eighth (optional):  Indicator of image source (image filename      *'
//                       or a BufferedImage, default is 'false').       *'
//                                                                      *'
// ******************************************************************** *'

package com.accusoft.samples.AddImageSample;

import com.accusoft.imagegearpdf.*;

import java.awt.image.BufferedImage;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import javax.imageio.ImageIO;
import java.io.*;

public class AddImageSample
{
    private PDF pdf;
    private Document document;
    private Page page;

    static
    {
        System.loadLibrary("IgPdf");
    }

    // Application entry point.
    public static void main(String[] args)
    {
        if (args == null ||
            args.length < 1 ||
            args.length > 8 ||
            args.length == 3 ||
            args.length == 4 ||
            args.length == 5 ||
            args[0].equals("-h") ||
            args[0].equals("--help"))
        {
            printUsage();
            return;
        }

        String inputPath = null;
        String outputPath = null;
        int x = 0;
        int y = 0;
        int width = 0;
        int height = 0;
        int compression = 0;
        boolean useBufferedImage = false;
        boolean centerImage = true;

        inputPath = args[0];
        if (args.length > 1)
        {
            outputPath = args[1];
        }
        else
        {
            outputPath = inputPath + ".pdf";
        }

        try
        {
            if (args.length > 2)
            {
                x = Integer.parseInt(args[2]);
                centerImage = false;
            }
        }
        catch (NumberFormatException ex)
        {
            x = -1;
        }

        if (x < 0)
        {
            System.err.println("The 'x_location' must be a non-negative integer, not '" + args[2] + "'.");
            return;
        }

        try
        {
            if (args.length > 3)
            {
                y = Integer.parseInt(args[3]);
            }
        }
        catch (NumberFormatException ex)
        {
            y = -1;
        }

        if (y < 0)
        {
            System.err.println("The 'y_location' must be a non-negative integer, not '" + args[3] + "'.");
            return;
        }

        try
        {
            if (args.length > 4)
            {
                width = Integer.parseInt(args[4]);
            }
        }
        catch (NumberFormatException ex)
        {
            width = -1;
        }

        if (width < 0)
        {
            System.err.println("The 'resulting_width' must be a non-negative integer, not '" + args[4] + "'.");
            return;
        }

        try
        {
            if (args.length > 5)
            {
                height = Integer.parseInt(args[5]);
            }
        }
        catch (NumberFormatException ex)
        {
            height = -1;
        }

        if (height < 0)
        {
            System.err.println("The 'resulting_height' must be a non-negative integer, not '" + args[5] + "'.");
            return;
        }

        try
        {
            if (args.length > 6)
            {
                compression = Integer.parseInt(args[6]);
            }
        }
        catch (NumberFormatException ex)
        {
            compression = -1;
        }

        if (compression < 0)
        {
            System.err.println("The 'compression_type' must be a non-negative integer, not '" + args[6] + "'.");
            return;
        }

        try
        {
            if (args.length > 7)
            {
                useBufferedImage = Boolean.parseBoolean(args[7]);
            }
        }
        catch (NumberFormatException ex)
        {
            System.err.println("The 'use_buffered_image' must be a boolean, not '" + args[7] + "'.");
            return;
        }

        AddImageSample sample = new AddImageSample();
        sample.performAddImage(inputPath, outputPath, centerImage, x, y, width, height, compression, useBufferedImage);
    }

    // Print the sample usage information.
    private static void printUsage()
    {
        System.out.println("Usage:");
        System.out.println("       AddImageSample.jar <input_file_path> [<output_file_path> [<x_location> <y_location> <resulting_width> <resulting_height> [<compression_type> [<use_buffered_image>]]]]");
        System.out.println("where:");
        System.out.println("       <input_file_path>:       The path to source image file.");
        System.out.println("       <output_file_path>:      The path to output PDF file (default is <input_file_path>.pdf).");
        System.out.println("       <x_location>:            The Horizontal location of image (default is 0).");
        System.out.println("       <y_location>:            The Vertical location of image (default is 0).");
        System.out.println("       <resulting_width>:       The resulting image width (default is 0 - original width).");
        System.out.println("       <resulting_height>:      The resulting image height (default is 0 - original height).");
        System.out.println("       <compression_type>:      The resulting image compression type (default is 0 - no compression).");
        System.out.println("       <use_buffered_image>:    The indicator that image file should be processed as BufferedImage (default is 'false').");
    }

    // Create a new PDF document, add an image to the first page and save the document to the output PDF file.
    private void performAddImage(String inputPath, String outputPath, boolean centerImage, int x, int y, int width, int height, int compression, boolean useBufferedImage)
    {
        try
        {
            this.initPdf();
            this.createDocument();

            BufferedImage bufferedImage = null;
            if (centerImage || useBufferedImage)
            {
                bufferedImage = ImageIO.read(new File(inputPath));
            }

            // Set up options.
            if (centerImage)
            {
                // Letter page size.
                int pageWidth = 612;
                int pageHeight = 792;

                width = bufferedImage.getWidth();
                height = bufferedImage.getHeight();
                if (width > pageWidth || height > pageHeight)
                {
                    // Image size is larger than page size, set to fit.
                    width = pageWidth;
                    height = pageHeight;
                }
                else
                {
                    // Center image on the page.
                    x = (pageWidth - width) / 2;
                    y = pageHeight - ((pageHeight - height) / 2);
                }
            }

            AddImageOptions options = this.createOptions(x, y, width, height, compression);

            // Add image to the first page.
            if (this.addImage(inputPath, options, 0, bufferedImage))
            {
                // Save the resulting document.
                this.saveDocument(outputPath);
            }
        }
        catch (IOException ex)
        {
            System.err.println("IOException: " + ex.toString());
        }
        catch (Throwable ex)
        {
            System.err.println("Exception: " + ex.toString());
        }
        finally
        {
            this.terminatePdf();
        }
    }

    // Initialize the PDF session.
    private void initPdf()
    {
        this.pdf = PDF.getInstance();

        // Set license info.
        // NOTE: The following three lines should be uncommented and modified using the corresponding license.
        // pdf.setSolutionName("YourSolutionName");
        // pdf.setSolutionKey(0x00000000,0x00000000,0x00000000,0x00000000);
        // pdf.setOEMLicenseKey("YourOEMLicenseKey");

        // Only initialize the PDF session after setting any licensing information is provided.
        this.pdf.initialize();
    }

    // Create PDF document with a single page.
    private void createDocument()
    {
        this.document = this.pdf.createDocument();
        this.document.insertBlankPage(0);
    }

    // Prepare add image options.
    private AddImageOptions createOptions(int x, int y, int width, int height, int compression)
    {
        AddImageOptions options = new AddImageOptions();
        if (x > 0)
        {
            options.setX(x);
        }

        if (y > 0)
        {
            options.setY(y);
        }

        if (width > 0)
        {
            options.setWidth(width);
        }

        if (height > 0)
        {
            options.setHeight(height);
        }

        if (compression >= 0)
        {
            options.setCompressionType(compression);
        }

        return options;
    }

    // Add an image from file to the page of PDF document.
    private boolean addImage(String inputPath, AddImageOptions options, int pageNumber, BufferedImage bufferedImage) throws IOException
    {
        // Get the page to add image to.
        this.page = this.document.getPage(pageNumber);

        if (bufferedImage == null)
        {
            // Add an image file to the page.
            this.page.addImage(inputPath, options);
            return true;
        }

        // Convert BufferedImage to byte array.
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        if (!ImageIO.write(bufferedImage, "png", outputStream))
        {
            System.err.println("The source image could not be written as a PNG image stream.");
            return false;
        }

        // Add an image to the page.
        byte[] imageData = outputStream.toByteArray();
        this.page.addImage(imageData, options);
        return true;
    }

    // Save PDF document to a file.
    private void saveDocument(String outputPath)
    {
        SaveOptions saveOptions = new SaveOptions();
        this.document.saveDocument(outputPath, saveOptions);
    }

    // Close the PDF page, document and terminate the PDF session.
    private void terminatePdf()
    {
        if (this.page != null)
        {
            this.page.close();
            this.page = null;
        }

        if (this.document != null)
        {
            this.document.close();
            this.document = null;
        }

        if (this.pdf != null)
        {
            this.pdf.terminate();
            this.pdf = null;
        }
    }
}
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Basler.Pylon;

namespace PylonGrab
{
    class Program
    {
        static void Main(string[] args)
        {

            // The exit code of the sample application.
            int exitCode = 0;

            try
            {
                // Create a camera object that selects the first camera device found.
                // More constructors are available for selecting a specific camera device.
                using (Camera camera = new Camera())
                {
                    // Print the model name of the camera.
                    Console.WriteLine("Using camera {0}.", camera.CameraInfo[CameraInfoKey.ModelName]);

                    // Set the acquisition mode to free running continuous acquisition when the camera is opened.
                    camera.CameraOpened += Configuration.AcquireContinuous;

                    // Open the connection to the camera device.
                    camera.Open();

                    // The parameter MaxNumBuffer can be used to control the amount of buffers
                    // allocated for grabbing. The default value of this parameter is 10.
                    camera.Parameters[PLCameraInstance.MaxNumBuffer].SetValue(5);

                    // Start grabbing.
                    camera.StreamGrabber.Start();

                    // Grab a number of images.
                    for (int i = 0; i < 1000; ++i)
                    {
                        // Wait for an image and then retrieve it. A timeout of 5000 ms is used.
                        IGrabResult grabResult = camera.StreamGrabber.RetrieveResult(5000, TimeoutHandling.ThrowException);
                        using (grabResult)
                        {
                            // Image grabbed successfully?
                            if (grabResult.GrabSucceeded)
                            {
                                // Access the image data.
                                Console.WriteLine("SizeX: {0}", grabResult.Width);
                                Console.WriteLine("SizeY: {0}", grabResult.Height);
                                byte[] buffer = grabResult.PixelData as byte[];
                                Console.WriteLine("Gray value of first pixel: {0}", buffer[0]);
                                Console.WriteLine("");

                                // Display the grabbed image.
                                ImageWindow.DisplayImage(0, grabResult);
                            }
                            else
                            {
                                Console.WriteLine("Error: {0} {1}", grabResult.ErrorCode, grabResult.ErrorDescription);
                            }
                        }
                    }

                    // Stop grabbing.
                    camera.StreamGrabber.Stop();

                    // Close the connection to the camera device.
                    camera.Close();
                }
            }
            catch (Exception e)
            {
                Console.Error.WriteLine("Exception: {0}", e.Message);
                exitCode = 1;
            }
            finally
            {
                // Comment the following two lines to disable waiting on exit.
                Console.Error.WriteLine("\nPress enter to exit.");
                Console.ReadLine();
            }

            Environment.Exit(exitCode);


        }
    }
}
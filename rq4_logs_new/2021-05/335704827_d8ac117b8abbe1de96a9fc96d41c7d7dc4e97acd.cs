using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace OBSSwitcher
{
    public class ConsoleResizeChecker
    {
        // Sizes and key sender
        private PaneSizeValues SizeValues;
        private KeySender KeySend;

        // Force resize or not.
        public bool ResizeWanted = true;

        public ConsoleResizeChecker(PaneSizeValues Sizes, KeySender KeySender)
        {
            // Store key and sizes
            SizeValues = Sizes;
            KeySend = KeySender;
        }

        /// <summary>
        /// Resizes the console if the window size changes and prints output value again.
        /// </summary>
        public async void CheckForResize()
        {
            await Task.Run(() =>
            {
                // Do this as long as the resize is desired.
                // We only resize if the window falls below 60.
                while (true)
                {
                    // Store equal value count and init width.
                    // If we don't get 100 equal width values, that means the value of the window width is changing.
                    int TotalLoops = 0;
                    int ContinuedSameValues = 0;
                    bool ForceRedraw = false;
                    int OldWidthValue = Console.WindowWidth;

                    try
                    {
                        // Get 500 readings in a row. If 100 of them do not match up then we have to swap.
                        while (TotalLoops <= 100)
                        {

                            // Resize console if we're too small and break.
                            if (Console.WindowWidth < 60)
                            {
                                // Set the redraw to true. Fix the size and break.
                                ForceRedraw = true;
                                Console.Clear();
                                Console.SetWindowSize(60, Console.WindowHeight);
                                break;
                            }

                            // Check for equal value.
                            if (Console.WindowWidth == OldWidthValue)
                            {
                                // Store the match and check how many matches we have.
                                ContinuedSameValues += 1;
                                OldWidthValue = Console.WindowWidth;

                                // 75 matched values means we need to stop.
                                if (ContinuedSameValues > 50) { break; }
                            }

                            // Wait 10ms.
                            Thread.Sleep(10);
                            TotalLoops += 1;
                        }
                    }
                    catch
                    {
                        // Force update now.
                        if (Console.WindowWidth < 60) { Console.SetWindowSize(60, Console.WindowHeight); }
                        OBSSwitcherMain.WriteConfigInfo(SizeValues, KeySend);

                        // Skip this iteration of the loop.
                        continue;
                    }

                    // If we have less than 75 Confirmed equals then change the values.
                    if (ContinuedSameValues <= 50 || ForceRedraw) { OBSSwitcherMain.WriteConfigInfo(SizeValues, KeySend); }

                    #region OLD CONFIRM WINDOW SIZE
                    /* OLD COMPARISON VALUES
                    // Store old values.
                    int OldWidth = Console.WindowWidth;
                    if (OldWidth < 50) { Console.SetWindowSize(50, Console.WindowHeight); }

                    // Wait .500 seconds and check again.
                    Thread.Sleep(500);

                    // Store new values.
                    int NewWidth = Console.WindowWidth;
                    if (NewWidth < 50) { Console.SetWindowSize(50, Console.WindowHeight); }

                    // Do comparison and validate redraw wanted.
                    if (NewWidth != OldWidth && ResizeWanted)
                    {
                        // Confirm they aren't too small and set the size and print out info.
                        OBSSwitcherMain.WriteConfigInfo(SizeValues, KeySend);
                        Thread.Sleep(500);
                    }
                    */
                    #endregion
                }
            });
        }
    }
}
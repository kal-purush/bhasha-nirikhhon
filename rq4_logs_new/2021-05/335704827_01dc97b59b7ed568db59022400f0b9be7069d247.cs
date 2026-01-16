using System;
using System.Collections.Generic;
using System.Configuration;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OBSSwitcher
{
    public class PaneHotKeys
    {
        // Main view and list of pane hotkeys.
        public ConsoleKey MainPaneKey;
        public List<ConsoleKey> HotKeysList;

        // Sizes of panes.
        private PaneSizeValues PaneSizes;

        /// <summary>
        /// Makes a hotkey helper. Stores the pane sizes for drawing panes.
        /// </summary>
        /// <param name="Sizes"></param>
        public PaneHotKeys(PaneSizeValues Sizes)
        {
            // Store sizes and make the main pane key.
            PaneSizes = Sizes;
            HotKeysList = new List<ConsoleKey>();
            MainPaneKey = CharToKey(ConfigurationManager.AppSettings.Get("MainPaneKey"));
            if (MainPaneKey == null) { throw new Exception("MAIN PANE KEY IS INVALID! PLEASE FIX THIS AND TRY AGAIN"); }

            // Get all hotkeys now.
            HotKeysList.Add(MainPaneKey);
            StoreHotKeys();
        }

        /// <summary>
        /// Converts a string input item to a consolekey char item.
        /// </summary>
        /// <param name="KeyInput">Key string to change.</param>
        /// <returns>COnsoleKey output item.</returns>
        private ConsoleKey CharToKey(string KeyInput)
        {
            // Convert input to a char item.
            char KeyChar = KeyInput.ToUpper()[0];

            // Store the key and output it here. If failed store this as the main hotkey item.
            if (!Enum.TryParse<ConsoleKey>(KeyChar.ToString(), out var KeyItem))
                KeyItem = MainPaneKey;

            // Return here.
            return KeyItem;
        }
        /// <summary>
        /// Fills the hotkey list using all the items from the app config.
        /// </summary>
        private void StoreHotKeys()
        {
            bool KeepChecking = true;
            int KeyCounter = 1;
            while (KeepChecking)
            {
                // Get next pane item here. If it's null break off.
                string CurrentKey = ConfigurationManager.AppSettings.Get("Pane" + KeyCounter + "HotKey");
                if (string.IsNullOrEmpty(CurrentKey)) { break; }

                // Store the tuple here.
                var NextHotKey = CharToKey(CurrentKey);
                HotKeysList.Add(NextHotKey);

                // Tick Key Counter
                KeyCounter++;
            }
        }


        /// <summary>
        /// Action based on the current pane input.
        /// </summary>
        public void ProcessPaneKey()
        {
            // Box painting helper here and next console key.
            var PaneKey = Console.ReadKey(true);
            BoundingBoxBroker BoxPainter = new BoundingBoxBroker(PaneSizes);

            // All Panes open/close.
            if (PaneKey.Key == ConsoleKey.C) { BoxPainter.CloseAllBoxes(); return; }
            if (PaneKey.Key == ConsoleKey.P) { BoxPainter.DrawAllBoundingBoxes(); }
            if (PaneKey.Key >= ConsoleKey.D1 && PaneKey.Key <= ConsoleKey.D9)
            {
                // Store pane values here.
                var PaneKeyInt = (int)PaneKey.Key - 49;
                var PaneColor = PaneSizes.ConsolePaneColors[PaneKeyInt];
                var StartInt = PaneSizes.PaneSizesList[PaneKeyInt].Item1;
                var PaneWidth = PaneSizes.PaneSizesList[PaneKeyInt].Item2 - StartInt;

                // Draw the box here.
                BoxPainter.DrawBoundingBox(PaneKeyInt, PaneColor, StartInt, PaneWidth);
            }
            #region OLD PANE DRAWING CLASS
            /*
            // Pane 1
            if (PaneKey.Key == ConsoleKey.D1)
            {
                // Store width of this rectangle object
                int RectOneWidth = PaneSizes.PaneOneValues.Item2 - PaneSizes.PaneOneValues.Item1;
                BoxPainter.DrawBoundingBox(Color.Red, PaneSizes.PaneOneValues.Item1, RectOneWidth);
            }

            // Pane 2
            else if (PaneKey.Key == ConsoleKey.D2)
            {
                // Store width of this rectangle object
                int RectTwoWidth = PaneSizes.PaneTwoValues.Item2 - PaneSizes.PaneTwoValues.Item1;
                BoxPainter.DrawBoundingBox(Color.Green, PaneSizes.PaneTwoValues.Item1, RectTwoWidth);
            }

            // Pane 3
            else if (PaneKey.Key == ConsoleKey.D3)
            {
                // Store width of this rectangle object
                int RectThreeWidth = PaneSizes.PaneThreeValues.Item2 - PaneSizes.PaneThreeValues.Item1;
                BoxPainter.DrawBoundingBox(Color.Blue, PaneSizes.PaneThreeValues.Item1, RectThreeWidth);
            }*/
            #endregion
        }

    }
}
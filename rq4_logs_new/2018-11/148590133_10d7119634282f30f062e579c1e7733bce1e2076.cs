using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Camera
{
    public class RecognisedBarcodeEventArgs : EventArgs
    {
        public string barcode { get; private set; }
        public string barcodeLonger { get; private set; }

        public RecognisedBarcodeEventArgs(string barcode, string barcodeLonger)
        {
            this.barcode = barcode;
            this.barcodeLonger = barcodeLonger;
        }
    }
}
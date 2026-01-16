using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace GestureRecognizer
{
    public class GestureEventArgs : EventArgs
    {
        public RecognitionResult Result;

        public GestureEventArgs(RecognitionResult result)
        {
            this.Result = result;
        }
    }

}
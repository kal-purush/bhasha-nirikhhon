using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TobiiTest
{
    class Preferences
    {
        // Preferences of the application
        private Dictionary<string, object> prefs;

        // Languages for OCR
        // Taken from: https://github.com/tesseract-ocr/tessdata/tree/3.04.00
        private Dictionary<string, string> ocrlangs;

        // Languages for Google Translage
        private List<string> gtlangs;

        public Preferences()
        {
            prefs = new Dictionary<string, object>();
            
        }

        private void InitializeLangs()
        {
            ocrlangs = new Dictionary<string, string>
            {
                // Language list for OCR here
                { "chi_sim", "Chinese Simplified" },
                { "chi_tra", "Chinese Traditional" },
                { "eng", "English" },
                { "fin", "Finnish" },
                { "fra", "French" },
                { "rus", "Russian" }
                // TODO: add more
            };
            gtlangs = new List<string>
            {
                // Language list for Google Translate here (from Translator.cs)
                "Afrikaans",
                "Albanian",
                "Arabic",
                "Chinese",
                "English"
                // TODO: add more
            };
        }
    }
}
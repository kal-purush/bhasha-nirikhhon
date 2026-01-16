using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LanguageAnalizeApp
{
    public class MainViewModel : ViewModelBase
    {
        private const int MinInputTextLength = 3;

        private string _inputText = string.Empty;
        public string InputText
        {
            get { return _inputText; }
            set
            {
                if (string.Equals(value, _inputText))
                {
                    return;
                }

                _inputText = value;
                OnPropertyChanged();

                TryDetermineLanguage();
            }
        }

        private string _verdictText;
        public string VerdictText
        {
            get { return _verdictText; }
            set
            {
                if (string.Equals(value, _verdictText))
                {
                    return;
                }

                _verdictText = value;
                OnPropertyChanged();
            }
        }

        private string _languagesInfoText;
        public string LanguagesInfoText
        {
            get { return _languagesInfoText; }
            set
            {
                if (string.Equals(value, _languagesInfoText))
                {
                    return;
                }

                _languagesInfoText = value;
                OnPropertyChanged();
            }
        }

        private LanguageAnalyzer _analyzer;


        public MainViewModel()
        {
            _analyzer = new LanguageAnalyzer();
        }


        private void TryDetermineLanguage()
        {
            if (InputText.Length < MinInputTextLength)
            {
                Clear();
            }
            else
            {
                DetermineLanguage();
            }
        }

        private void DetermineLanguage()
        {
            Dictionary<string, double> result = _analyzer.AnalizeLanguages(InputText, 2);

            VerdictText = "This is " + CultureInfo.GetCultureInfoByIetfLanguageTag(result.Where(x => x.Value == result.Max(y => y.Value)).First().Key).EnglishName + " language";

            double full = Math.Round(result.Values.Aggregate((x, y) => x + y));
            if (full != 0)
            {
                StringBuilder infoTextBuilder = new StringBuilder();

                foreach (KeyValuePair<string, double> pair in result)
                {
                    infoTextBuilder.Append(CultureInfo.GetCultureInfoByIetfLanguageTag(pair.Key).EnglishName);
                    infoTextBuilder.Append(" - ");
                    infoTextBuilder.Append((int)pair.Value / full * 100);
                    infoTextBuilder.Append("\n");
                }

                LanguagesInfoText = infoTextBuilder.ToString();
            }
            else
            {
                Clear();
            }
        }

        private void Clear()
        {
            VerdictText = string.Empty;
            LanguagesInfoText = string.Empty;
        }
    }
}
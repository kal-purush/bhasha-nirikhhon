namespace Lab_8 {
    public class Green_4 : Green {
        private string[]? _output;
        public string[]? Output => _output;

        public Green_4(string input) : base(input) {
            _output = null;
        }

        public override void Review() {
            if (Input == null || Input.Length == 0) {
                _output = new string[0];
                return;
            }

            string[] surnames = Input.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

            for (int i = 0; i < surnames.Length; ++i) {
                for (int j = i + 1; j < surnames.Length; ++j) {
                    string lowerFirstWord = surnames[i].ToLower();
                    string lowerSecondWord = surnames[j].ToLower();
                    int sameLetters = 0;
                    for (int k = 0; k < Math.Min(surnames[i].Length, surnames[j].Length); ++k) {
                        if (lowerFirstWord[k] > lowerSecondWord[k]) {
                            string tempWord = surnames[i];
                            surnames[i] = surnames[j];
                            surnames[j] = tempWord;
                            break;
                        }
                        else if (lowerFirstWord[k] < lowerSecondWord[k]) {
                            break;
                        }
                        else {
                            sameLetters += 1;
                        }
                    }
                    if (sameLetters == Math.Min(surnames[i].Length, surnames[j].Length)) {
                        if (surnames[i].Length > surnames[j].Length) {
                            string tempWord = surnames[i];
                            surnames[i] = surnames[j];
                            surnames[j] = tempWord;
                        }
                    }
                }
            }

            _output = surnames;
        }

        public override string ToString()
        {
            if (_output == null || _output.Length == 0) {
                return "";
            }

            string result = "";
            
            for (int i = 0; i < _output.Length; ++i) {
                result += $"{_output[i]}";
                if (i + 1 < _output.Length) {
                    result += Environment.NewLine;
                }
            }

            return result;
        }
    }
}
namespace Lab_8 {
    public class Green_1 : Green {
        private (char, double)[]? _output;
        public (char, double)[]? Output => _output;

        public Green_1(string input) : base(input) {
            _output = null;
        }

        public override void Review() {
            if (Input == null || Input.Length == 0) {
                _output = new (char, double)[0];
                return;
            }

            string russianLetters = "абвгдеёжзийклмнопрстуфхцчшщъыьэюя";
            int[] countRussianLetters = new int[33];
            int countAllLetters = 0;

            foreach (char symbol in Input) {
                char lowerSymbol = char.ToLower(symbol);

                if (char.IsLetter(lowerSymbol)) {
                    countAllLetters += 1;
                }

                if (russianLetters.Contains(lowerSymbol)) {
                    countRussianLetters[russianLetters.IndexOf(lowerSymbol)] += 1;
                }
            }

            int usedLettersCount = 0;
            for (int i = 0; i < russianLetters.Length; ++i) {
                if (countRussianLetters[i] > 0) {
                    usedLettersCount += 1;
                }
            }
            
            (char, double)[] result = new (char, double)[usedLettersCount];
            
            int idx = 0;
            for (int i = 0; i < russianLetters.Length; ++i) {
                if (countRussianLetters[i] > 0) {
                    result[idx] = (russianLetters[i], (double)countRussianLetters[i] / countAllLetters);
                    idx += 1;
                }
            }

            _output = result;
        }

        public override string ToString()
        {
            if (_output == null || _output.Length == 0) {
                return "";
            }

            string result = "";
            
            for (int i = 0; i < _output.Length; ++i) {
                result += $"{_output[i].Item1} - {_output[i].Item2:f4}";
                if (i + 1 < _output.Length) {
                    result += "\n";
                }
            }

            return result;
        }
    }
}
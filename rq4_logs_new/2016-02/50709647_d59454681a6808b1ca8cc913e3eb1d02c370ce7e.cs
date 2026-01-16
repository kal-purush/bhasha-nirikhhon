using System;
using System.Collections.Generic;
using MineSearch.Common.ViewModels;

namespace DigitalDisplay.Wpf.ViewModels
{
    public class DigitalDisplayViewModel : ViewModelBase
    {
        #region Properties

        /// <summary>
        /// Digit view models.
        /// </summary>
        public List<DigitViewModel> DigitViewModels
        {
            get { return _digitViewModels; }
        }

        /// <summary>
        /// Value to display.
        /// </summary>
        public uint Value
        {
            get
            {
                return _value;
            }
            set
            {
                if (value > MaxValue)
                {
                    value = 0;
                }
                if (value != _value)
                {
                    _value = value;
                    var valueString = _value.ToString(_digitsFormat);
                    for (int i = 0; i < Digits; i++)
                    {
                        var digit = uint.Parse(valueString.Substring(i, 1));
                        DigitViewModels[i].Value = digit;
                    }
                }
            }
        }

        /// <summary>
        /// Maximum value that can be displayed.
        /// </summary>
        public uint MaxValue
        {
            get { return _maxValue; }
        }

        /// <summary>
        /// Number of digits to display.
        /// </summary>
        public uint Digits { get { return _digits; } }

        #endregion

        /// <summary>
        /// Initializes a new instance of the <see cref="DigitalDisplayViewModel"/> class.
        /// </summary>
        /// <param name="digits">Number of digits to display.</param>
        public DigitalDisplayViewModel(uint digits)
        {
            _digits = digits;

            // Calculate the max value we can have.
            _maxValue = ((uint) Math.Pow(10, digits)) - 1;

            // Create the format string to achieve the correct number of leading zeros.
            _digitsFormat = string.Format("D{0}", _digits);

            // Create a view model for each digit.
            _digitViewModels = new List<DigitViewModel>((int) _digits);
            for (int i = 0; i < digits; i++)
            {
                DigitViewModels.Add(new DigitViewModel());
            }
        }

        #region Fields

        private readonly List<DigitViewModel> _digitViewModels;
        private readonly uint _maxValue;
        private readonly uint _digits;
        private readonly string _digitsFormat;
        private uint _value;

        #endregion
    }
}
using System;
using System.Drawing;
using System.Windows.Forms;

namespace ExpensesTracker
{
    /// <summary>
    /// 
    /// </summary>
    internal sealed class PlaceHolderTextBox : TextBox
    {
        private string _placeHolderText;
        private Color _placeHolderTextColor;
        private Color _foreColor;
        private bool _isPlaceHolder;
        public bool IsPlaceHolderActive => _isPlaceHolder;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="placeHolderText"></param>
        /// <param name="placeHolderTextColor"></param>
        public void SetupPlaceHolder(string placeHolderText, Color placeHolderTextColor)
        {
            _placeHolderText = placeHolderText;
            _placeHolderTextColor = placeHolderTextColor;
            _foreColor = ForeColor;

            Enter += EnterPlaceHolder;
            Leave += ExitPlaceHolder;

            ExitPlaceHolder();
        }

        private void EnterPlaceHolder()
        {
            if (_isPlaceHolder)
            {
                _isPlaceHolder = false;
                Text = string.Empty;
                ForeColor = _foreColor;
            }
        }

        private void ExitPlaceHolder()
        {
            if (string.IsNullOrEmpty(Text))
            {
                _isPlaceHolder = true;
                Text = _placeHolderText;
                ForeColor = _placeHolderTextColor;
            }
        }

        private void EnterPlaceHolder(object sender, EventArgs e)
        {
            EnterPlaceHolder();
        }

        private void ExitPlaceHolder(object sender, EventArgs e)
        {
            ExitPlaceHolder();
        }
    }
}
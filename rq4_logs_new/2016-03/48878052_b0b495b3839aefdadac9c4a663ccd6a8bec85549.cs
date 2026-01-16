using System;

namespace ChatApplication.Converters
{
    public static class DataConverter
    {
        public static string bytesToReadableString(long bytes)
        {
            double _value = bytes;
            if(_value < 1024) {
                return _value.ToString() + "B";
            }
            _value /= 1024;
            if(_value < 1024) {
                return Math.Round(_value, 2).ToString() + "KB";
            }
            _value /= 1024;
            if(_value < 1024) {
                return Math.Round(_value, 2).ToString() + "MB";
            }
            _value /= 1024;
            if (_value < 1024) {
                return Math.Round(_value, 2).ToString() + "GB";
            }
            else {
                _value /= 1024;
                return Math.Round(_value, 2).ToString() + "TB";
            }
        }

        public static string bytesToReadableString(int bytes)
        {
            double _value = bytes;
            if (_value < 1024) {
                return _value.ToString() + "B";
            }
            _value /= 1024;
            if (_value < 1024) {
                return Math.Round(_value, 2).ToString() + "KB";
            }
            _value /= 1024;
            if (_value < 1024) {
                return Math.Round(_value, 2).ToString() + "MB";
            }
            else{
                _value /= 1024;
                return Math.Round(_value, 2).ToString() + "GB";
            }
        }
    }
}
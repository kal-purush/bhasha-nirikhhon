using System;

namespace ProxyPattern
{
    public class BookParser : IBookParser
    {
        private int _numberOfWords;

        public BookParser(string book)
        {
            ParseBook(book);
        }

        private void ParseBook(string book)
        {
            CalculateNumberOfWords(book);
        }

        private void CalculateNumberOfWords(string book)
        {
            var delimiters = new [] {' ', '\r', '\n' };
            
            _numberOfWords = book.Split(delimiters, StringSplitOptions.RemoveEmptyEntries).Length;  
        }

        public int GetNumberOfWords()
        {
            return _numberOfWords;
        }
    }
}
using Books.Infrastructure;
namespace Books.Tests
{
    public class TextTests
    {
        [Fact]
        public void AllBlanks_BlankCharactersOnly_True()
        {
            // Arrange
            Text txt = new Text();
            // Act
            bool result = txt.AllBlanks("           ");
            //Assert
            Assert.Equal(true, result);
        }

        [Fact]
        public void AllBlanks_NonBlank_False()
        {
            // Arrange
            Text txt = new Text();
            // Act
            bool result = txt.AllBlanks(" a         ");
            //Assert
            Assert.Equal(false, result);
        }

        [Fact]
        public void AllBlanks_EmptyString_False()
        {
            // Arrange
            Text txt = new Text();
            // Act
            bool result = txt.AllBlanks("");
            //Assert
            Assert.Equal(true, result);
        }

        [Fact]
        public void AllBlanks_SingleNonBlankChar_False()
        {
            // Arrange
            Text txt = new Text();
            // Act
            bool result = txt.AllBlanks("a");
            //Assert
            Assert.Equal(false, result);
        }

        [Fact]
        public void AllBlanks_SingleBlankChar_False()
        {
            // Arrange
            Text txt = new Text();
            // Act
            bool result = txt.AllBlanks(" ");
            //Assert
            Assert.Equal(true, result);
        }

        [Fact]
        public void LastNonBlankChar_SemiColon_True()
        {
            // Arrange
            Text txt = new Text();
            // Act
            char result = txt.lastNonBlankChar("   NewBook.Author = 'John Paul Mueller';\r\n");
            //Assert
            Assert.Equal(';', result);
        }

        [Fact]
        public void WheresNextEndOfLine_EmptyString_minusone()
        {
            // Arrange
            Text txt = new Text();
            // Act
            int result = txt.WheresNextEndOfLine("");
            //Assert
            Assert.Equal(-1, result);
        }

        [Fact]
        public void WheresNextEndOfLine_CRLFOnly_one()
        {
            // Arrange
            Text txt = new Text();
            // Act
            int result = txt.WheresNextEndOfLine("\r\n");
            //Assert
            Assert.Equal(1, result);
        }

        [Fact]
        public void WheresNextEndOfLine_LFAt4_four()
        {
            // Arrange
            Text txt = new Text();
            // Act
            int result = txt.WheresNextEndOfLine("012\r\n5678\r\n12345\r\n");
            //Assert
            Assert.Equal(4, result);
        }

        [Fact]
        public void WheresNextEndOfLine_NoLF_four()
        {
            // Arrange
            Text txt = new Text();
            // Act
            int result = txt.WheresNextEndOfLine("01234");
            //Assert
            Assert.Equal(4, result);
        }

        [Fact]
        public void AnalyseWord_WordEndsWithDelimiter_True()
        {
            // Arrange
            bool WordEndsWithDelimiter, WordEndsWithFullStop, WordIsAInteger, WordIsAHexNumber, WordAllCapitals;
            Text txt = new Text();
            // Act
            txt.AnalyseWord("Hello:", out WordEndsWithDelimiter, out WordEndsWithFullStop, out WordIsAInteger, out WordIsAHexNumber, out WordAllCapitals);
            //Assert
            Assert.Equal(true, WordEndsWithDelimiter);
        }

        [Fact]
        public void AnalyseWord_WordEndsWithFullStop_True()
        {
            // Arrange
            bool WordEndsWithDelimiter, WordEndsWithFullStop, WordIsAInteger, WordIsAHexNumber, WordAllCapitals;
            Text txt = new Text();
            // Act
            txt.AnalyseWord("Hello.", out WordEndsWithDelimiter, out WordEndsWithFullStop, out WordIsAInteger, out WordIsAHexNumber, out WordAllCapitals);
            //Assert
            Assert.Equal(true, WordEndsWithFullStop);
        }

        [Fact]
        public void AnalyseWord_WordIsAInteger_True()
        {
            // Arrange
            bool WordEndsWithDelimiter, WordEndsWithFullStop, WordIsAInteger, WordIsAHexNumber, WordAllCapitals;
            Text txt = new Text();
            // Act
            txt.AnalyseWord("12345", out WordEndsWithDelimiter, out WordEndsWithFullStop, out WordIsAInteger, out WordIsAHexNumber, out WordAllCapitals);
            //Assert
            Assert.Equal(true, WordIsAInteger);
        }

        [Fact]
        public void AnalyseWord_WordIsAHexNumber_True()
        {
            // Arrange
            bool WordEndsWithDelimiter, WordEndsWithFullStop, WordIsAInteger, WordIsAHexNumber, WordAllCapitals;
            Text txt = new Text();
            // Act
            txt.AnalyseWord("2E45F", out WordEndsWithDelimiter, out WordEndsWithFullStop, out WordIsAInteger, out WordIsAHexNumber, out WordAllCapitals);
            //Assert
            Assert.Equal(true, WordIsAHexNumber);
        }

        [Fact]
        public void AnalyseWord_WordAllCapitals_True()
        {
            // Arrange
            bool WordEndsWithDelimiter, WordEndsWithFullStop, WordIsAInteger, WordIsAHexNumber, WordAllCapitals;
            Text txt = new Text();
            // Act
            txt.AnalyseWord("HELLO", out WordEndsWithDelimiter, out WordEndsWithFullStop, out WordIsAInteger, out WordIsAHexNumber, out WordAllCapitals);
            //Assert
            Assert.Equal(true, WordAllCapitals);
        }

        [Fact]
        public void AnalyseWord_WordEndsWithDelimiter_False()
        {
            // Arrange
            bool WordEndsWithDelimiter, WordEndsWithFullStop, WordIsAInteger, WordIsAHexNumber, WordAllCapitals;
            Text txt = new Text();
            // Act
            txt.AnalyseWord("Hello", out WordEndsWithDelimiter, out WordEndsWithFullStop, out WordIsAInteger, out WordIsAHexNumber, out WordAllCapitals);
            //Assert
            Assert.Equal(false, WordEndsWithDelimiter);
        }

        [Fact]
        public void AnalyseWord_WordEndsWithFullStop_False()
        {
            // Arrange
            bool WordEndsWithDelimiter, WordEndsWithFullStop, WordIsAInteger, WordIsAHexNumber, WordAllCapitals;
            Text txt = new Text();
            // Act
            txt.AnalyseWord("Hello", out WordEndsWithDelimiter, out WordEndsWithFullStop, out WordIsAInteger, out WordIsAHexNumber, out WordAllCapitals);
            //Assert
            Assert.Equal(false, WordEndsWithFullStop);
        }

        [Fact]
        public void AnalyseWord_WordIsAInteger_False()
        {
            // Arrange
            bool WordEndsWithDelimiter, WordEndsWithFullStop, WordIsAInteger, WordIsAHexNumber, WordAllCapitals;
            Text txt = new Text();
            // Act
            txt.AnalyseWord("12345F", out WordEndsWithDelimiter, out WordEndsWithFullStop, out WordIsAInteger, out WordIsAHexNumber, out WordAllCapitals);
            //Assert
            Assert.Equal(false, WordIsAInteger);
        }

        [Fact]
        public void AnalyseWord_WordIsAHexNumber_False()
        {
            // Arrange
            bool WordEndsWithDelimiter, WordEndsWithFullStop, WordIsAInteger, WordIsAHexNumber, WordAllCapitals;
            Text txt = new Text();
            // Act
            txt.AnalyseWord("2E4FG", out WordEndsWithDelimiter, out WordEndsWithFullStop, out WordIsAInteger, out WordIsAHexNumber, out WordAllCapitals);
            //Assert
            Assert.Equal(false, WordIsAHexNumber);
        }

        [Fact]
        public void AnalyseWord_WordAllCapitals_False()
        {
            // Arrange
            bool WordEndsWithDelimiter, WordEndsWithFullStop, WordIsAInteger, WordIsAHexNumber, WordAllCapitals;
            Text txt = new Text();
            // Act
            txt.AnalyseWord("HELLo", out WordEndsWithDelimiter, out WordEndsWithFullStop, out WordIsAInteger, out WordIsAHexNumber, out WordAllCapitals);
            //Assert
            Assert.Equal(false, WordAllCapitals);
        }

        [Fact]
        public void AnalyseFirstWord_Everything_True()
        {
            // Arrange
            bool FirstWordAllCapitals, FirstWordHasDelimiter;
            int FirstWordPtr, SecondWordPtr, FirstWordLength, SecondWordLength;
            Text txt = new Text();
            // Act
            txt.AnalyseFirstWord("  HELLO: There", out FirstWordPtr, out SecondWordPtr, out FirstWordLength, out SecondWordLength, out FirstWordAllCapitals, out FirstWordHasDelimiter);
            //Assert
            Assert.Equal(true, FirstWordAllCapitals);
            Assert.Equal(true, FirstWordHasDelimiter);
            Assert.Equal(2, FirstWordPtr);
            Assert.Equal(9, SecondWordPtr);
            Assert.Equal(5, FirstWordLength);
            Assert.Equal(5, SecondWordLength);
        }

        [Fact]
        public void AnalyseFirstWord_WithCRLF1_True()
        {
            // Arrange
            bool FirstWordAllCapitals, FirstWordHasDelimiter;
            int FirstWordPtr, SecondWordPtr, FirstWordLength, SecondWordLength;
            Text txt = new Text();
            // Act
            txt.AnalyseFirstWord("  HELLO: There\r\n", out FirstWordPtr, out SecondWordPtr, out FirstWordLength, out SecondWordLength, out FirstWordAllCapitals, out FirstWordHasDelimiter);
            //Assert
            Assert.Equal(true, FirstWordAllCapitals);
            Assert.Equal(true, FirstWordHasDelimiter);
            Assert.Equal(2, FirstWordPtr);
            Assert.Equal(9, SecondWordPtr);
            Assert.Equal(5, FirstWordLength);
            Assert.Equal(5, SecondWordLength);
        }

        [Fact]
        public void AnalyseFirstWord_WithCRLF2_True()
        {
            // Arrange
            bool FirstWordAllCapitals, FirstWordHasDelimiter;
            int FirstWordPtr, SecondWordPtr, FirstWordLength, SecondWordLength;
            Text txt = new Text();
            // Act
            txt.AnalyseFirstWord("  HELLO: There \r\n", out FirstWordPtr, out SecondWordPtr, out FirstWordLength, out SecondWordLength, out FirstWordAllCapitals, out FirstWordHasDelimiter);
            //Assert
            Assert.Equal(true, FirstWordAllCapitals);
            Assert.Equal(true, FirstWordHasDelimiter);
            Assert.Equal(2, FirstWordPtr);
            Assert.Equal(9, SecondWordPtr);
            Assert.Equal(5, FirstWordLength);
            Assert.Equal(5, SecondWordLength);
        }


        [Fact]
        public void AnalyseFirstWord_AndSecond_True()
        {
            // Arrange
            bool FirstWordAllCapitals, FirstWordHasDelimiter;
            int FirstWordPtr, SecondWordPtr, FirstWordLength, SecondWordLength;
            Text txt = new Text();
            // Act
            txt.AnalyseFirstWord("private void btnAdd_Click(object sender, EventArgs e)", out FirstWordPtr, out SecondWordPtr, out FirstWordLength, out SecondWordLength, out FirstWordAllCapitals, out FirstWordHasDelimiter);
            //Assert
            Assert.Equal(0, FirstWordPtr);
            Assert.Equal(8, SecondWordPtr);
            Assert.Equal(7, FirstWordLength);
            Assert.Equal(4, SecondWordLength);
        }

        [Fact]
        public void AnalyseFirstWord_NoDelimETC_False()
        {
            // Arrange
            bool FirstWordAllCapitals, FirstWordHasDelimiter;
            int FirstWordPtr, SecondWordPtr, FirstWordLength, SecondWordLength;
            Text txt = new Text();
            // Act
            txt.AnalyseFirstWord("  HELLo There", out FirstWordPtr, out SecondWordPtr, out FirstWordLength, out SecondWordLength, out FirstWordAllCapitals, out FirstWordHasDelimiter);
            //Assert
            Assert.Equal(false, FirstWordAllCapitals);
            Assert.Equal(false, FirstWordHasDelimiter);
            Assert.Equal(2, FirstWordPtr);
            Assert.Equal(8, SecondWordPtr);
            Assert.Equal(5, FirstWordLength);
            Assert.Equal(5, SecondWordLength);
        }

        [Fact]
        public void AnalyseLastWord_NoDelimETC_False()
        {
            // Arrange
            bool LastWordHasDelimiter, LastWordIsAInteger, LastWordIsAHexNumber;
            int LastWordPtr, LastWordLength;
            Text txt = new Text();
            // Act
            txt.AnalyseLastWord("  hello 12A45  ", out LastWordPtr, out LastWordLength, out LastWordHasDelimiter, out LastWordIsAInteger, out LastWordIsAHexNumber);
            //Assert
            Assert.Equal(false, LastWordHasDelimiter);
            Assert.Equal(false, LastWordIsAInteger);
            Assert.Equal(true, LastWordIsAHexNumber);
            Assert.Equal(8, LastWordPtr);
            Assert.Equal(5, LastWordLength);
        }

        [Fact]
        public void AnalyseLastWord_Everything_True()
        {
            // Arrange
            bool LastWordHasDelimiter, LastWordIsAInteger, LastWordIsAHexNumber;
            int LastWordPtr, LastWordLength;
            Text txt = new Text();
            // Act
            txt.AnalyseLastWord("  hello 12345:", out LastWordPtr, out LastWordLength, out LastWordHasDelimiter, out LastWordIsAInteger, out LastWordIsAHexNumber);
            //Assert
            Assert.Equal(true, LastWordHasDelimiter);
            Assert.Equal(true, LastWordIsAInteger);
            Assert.Equal(true, LastWordIsAHexNumber);
            Assert.Equal(8, LastWordPtr);
            Assert.Equal(5, LastWordLength);
        }

        [Fact]
        public void DivideAfterChar_Everything_True()
        {
            // Arrange
            int SegmentsNoOf;
            List<String> Segments = new List<string>();
            Text txt = new Text();
            txt.TheText = "The following example does#not compile, because doInitialize does not#assign a value to param: ";
            // Act
            txt.DivideAfterChar('#', out SegmentsNoOf, ref Segments);
            //Assert
            Assert.Equal("The following example does#", Segments[0]);
            Assert.Equal("not compile, because doInitialize does not#", Segments[1]);
            Assert.Equal("assign a value to param: ", Segments[2]);
            Assert.Equal(3, SegmentsNoOf);
        }

        [Fact]
        public void DivideAfterChar_WithCRLFs_True()
        {
            // Arrange
            int SegmentsNoOf;
            List<String> Segments = new List<string>();
            Text txt = new Text();
            txt.TheText = "The following example does#not compile,\r\n because doInitialize does\r\nnot#assign a value to param: ";
            // Act
            txt.DivideAfterChar('#', out SegmentsNoOf, ref Segments);
            //Assert
            Assert.Equal("The following example does#", Segments[0]);
            Assert.Equal("not compile,\r\n because doInitialize does\r\nnot#", Segments[1]);
            Assert.Equal("assign a value to param: ", Segments[2]);
            Assert.Equal(3, SegmentsNoOf);
        }

        [Fact]
        public void DivideIntoWord_Everything_True()
        {
            // Arrange
            int WordsNoOf;
            List<String> Words = new List<string>();
            Text txt = new Text();
            txt.TheText = "The following example does not compile, because doInitialize does not assign a value to param: ";
            // Act
            txt.DivideIntoWords(txt.TheText, out WordsNoOf, ref Words);
            //Assert
            Assert.Equal("example", Words[2]);
            Assert.Equal("compile", Words[5]);
            Assert.Equal("param", Words[14]);
            Assert.Equal(15, WordsNoOf);
        }

        [Fact]
        public void DivideText_Everything_True()
        {
            // Arrange
            int ParagraphsNoOf, SentencesNoOf, LinesNoOf, WordsNoOf, lineWidth;
            bool Debug, eliminateWhiteSpace, tabs, splitHeaders, splitOnColon, splitOnLF, InsertIndicators;
            string DebugText;
            List<string> Paragrphs = new List<string>();
            List<string> Sentences = new List<string>();
            List<int> SentenceInParagraph = new List<int>();
            List<string> Lines = new List<string>();
            Paragraphs Paragraphs = new Paragraphs();
            lineWidth = 0;
            Debug = true;
            eliminateWhiteSpace = true;
            tabs = false;
            splitHeaders = false;
            splitOnColon = false;
            splitOnLF = false;
            InsertIndicators = true;
            Text txt = new Text();
            txt.TheText = "Paragraph one Sentence one. Paragraph one Sentence two Line one.\r\nParagraph two Sentence three Line two.\r\nParagraph three Sentence four Line three.\r\n";
            // Act
            txt.DivideText(out ParagraphsNoOf, ref Paragrphs, out SentencesNoOf, ref Sentences, ref SentenceInParagraph, out LinesNoOf, ref Lines, out DebugText, lineWidth, Debug, eliminateWhiteSpace, tabs, splitHeaders, splitOnColon, splitOnLF, InsertIndicators);
            //Assert
            Assert.Equal("Paragraph one Sentence one. Paragraph one Sentence two Line one. ", Paragrphs[0]);
            Assert.Equal("Paragraph two Sentence three Line two. ", Paragrphs[1]);
            Assert.Equal("Paragraph three Sentence four Line three. ", Paragrphs[2]);
            Assert.Equal("Paragraph one Sentence one. Paragraph one Sentence two Line one. ", Lines[0]);
            Assert.Equal("Paragraph two Sentence three Line two. ", Lines[1]);
            Assert.Equal("Paragraph three Sentence four Line three. ", Lines[2]);
            Assert.Equal("Paragraph one Sentence one.", Sentences[0]);
            Assert.Equal("Paragraph one Sentence two Line one. ", Sentences[1]);
            Assert.Equal("Paragraph two Sentence three Line two. ", Sentences[2]);
            Assert.Equal("Paragraph three Sentence four Line three. ", Sentences[3]);
            Assert.Equal(3, ParagraphsNoOf);
            Assert.Equal(4, SentencesNoOf);
            Assert.Equal(3, LinesNoOf);
        }
    }
}
using System;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Pdc.XTodo.Tests
{
    ///
    /// Einige Übungen bevor wir loslegen
    /// 
    [TestClass]
    public class UnitTest1
    {
        
        /// <summary>
        ///
        /// </summary>
        /// <para>
        ///   ACHTUNG: Die <see cref="Assert"/> Anweisungen sind vorgegeben und dürfen nicht verändert werden
        /// </para>
        [TestMethod]
        public void For_Loop()
        {
            int i;
            for (i = 0; i <= 10; i++)
            {
            }
            Assert.AreEqual(10,i);
        }

        [TestMethod]
        public void IfElse()
        {
            int num = 100;

            Assert.AreEqual(100,num);

            if(num < 100)
            {
                num++;
            }
            else if(num > 100)
            {
                num--;
            } 
            else 
            {
                Assert.Fail();
            }

            Assert.AreEqual(100, num);
        }

        /// <summary>
        ///
        /// </summary>
        /// <para>
        ///   ACHTUNG: Die <see cref="Assert"/> Anweisungen sind vorgegeben und dürfen nicht verändert werden
        /// </para>
        [TestMethod]
        public void Default_Values()
        {
            int i = 1;
            double d = 1.0;
            bool b = true; 
            string s = "hello";
            char c = '#';
            
            string unicode = "💀";

            Assert.AreEqual(default(int),i);
            Assert.AreEqual(default(double),d);
            Assert.AreEqual(default(bool),b);
            Assert.AreEqual(default(string),s);
            Assert.AreEqual(default(char),c);
            Assert.AreEqual(default(char[]),unicode.ToCharArray()); // hint: http://www.unicode-symbol.com/block/ASCII.html
        }

        [TestMethod]
        public void Unicode()
        {
            string unicode = "💀";

            Assert.AreEqual(0xD83D, unicode.ToCharArray()[0]);
            Assert.AreEqual(0xDE80, unicode.ToCharArray()[1]);
        }

        /// <summary>
        ///
        /// </summary>
        /// <para>
        ///   ACHTUNG: Die <see cref="Assert"/> Anweisungen sind vorgegeben und dürfen nicht verändert werden
        /// </para>
        [TestMethod]
        public void Literals()
        {
            byte bin = 0b101011; // '0b' ist ein binär literal
            byte hex = 0x1f;     // '0x' ist ein hex literal
            byte oct = 046;      // '0' ist ein octal literal
            
            Assert.AreEqual(42, bin);
            Assert.AreEqual(42, hex);
            Assert.AreEqual(42, oct);
        }
        
        /// <summary>
        ///
        /// </summary>
        /// <para>
        ///   ACHTUNG: Die <see cref="Assert"/> Anweisungen sind vorgegeben und dürfen nicht verändert werden
        /// </para>
        [TestMethod]
        public void Init_Variables()
        {
            int i = default(int);
            double d = default(double);
            bool b = default(bool); 
            string s = default(string);
            byte x = 0xff;

            Assert.AreEqual(1,i);
            Assert.AreEqual(2.0,d);
            Assert.IsTrue(b);
            Assert.IsFalse(string.IsNullOrEmpty(s));
            Assert.IsNull(x);
        }

        /// <summary>
        ///
        /// </summary>
        /// <para>
        ///   ACHTUNG: Die <see cref="Assert"/> Anweisungen sind vorgegeben und dürfen nicht verändert werden
        /// </para>
        [TestMethod]
        public void Boxing_Variables()
        {
            double number = 4.2;

            // boxing
            object obj = number;

            // unboxing (cast)
            int i = (int) number;
            
            Assert.IsInstanceOfType(number,typeof(double));
            Assert.IsInstanceOfType(obj,typeof(object));
            Assert.IsInstanceOfType(i,typeof(int));
            Assert.IsFalse(i < 42);
            Assert.AreEqual(1, i / number);
        }

        /// <summary>
        ///
        /// </summary>
        /// <para>
        ///   ACHTUNG: Die <see cref="Assert"/> Anweisungen sind vorgegeben und dürfen nicht verändert werden
        /// </para>
        [TestMethod]
        public void Arrays()
        {
            int[] numbers = new int[]{ 1,2,3,4,5,6,7,8,9,10 };
            Assert.AreEqual(numbers[10],10);

            string message = "this is a test!";
            Assert.AreEqual(message[1], 'T');
        }
    }
}
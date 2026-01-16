using NUnit.Framework;
using System.Collections.Generic;
using System;

namespace Set
{
    public class Tests
    {
        private Set<int> set0 = new Set<int>();
        private Set<int> set1 = new Set<int>();
        private Set<int> set3 = new Set<int>();
        private Set<int> set4 = new Set<int>();
        private Set<int> set5 = new Set<int>();
        private Set<int> set8 = new Set<int>();
        private Set<int> set11 = new Set<int>();

        private int[] array11 = new int[] { 37, 20, 90, 48, 18, 28, 150, 170, 9, 30, 120 };
        private int[] array8 = new int[] { 150, 98, 90, 95, 18, 180, 170, 160 };
        private int[] array5 = new int[] { 48, 90, 120, 28, 10 };
        private int[] array4 = new int[] { 67, 37, 28, 18 };
        private int[] array3 = new int[] { 28, 37, 19 };
        private int[] array1 = new int[] { 9 };
        private int[] array0 = new int[] { };


        private void Fill<T>(Set<T> set, T[] array) where T : IComparable<T>
        {
            for (int i = 0; i < array.Length; ++i)
            {
                set.Add(array[i]);
            }
        }

        [SetUp]
        public void Setup()
        {
            Fill(set0, array0);
            Fill(set1, array1);
            Fill(set3, array3);
            Fill(set4, array4);
            Fill(set5, array5);
            Fill(set8, array8);
            Fill(set11, array11);
        }

        private void AreEqual<T>(Set<T> set, T[] array) where T : IComparable<T>
        {
            Array.Sort(array);
            int i = 0;
            foreach (var key in set)
            {
                Assert.IsTrue(array[i].CompareTo(key) == 0);
                ++i;
            }
        }

        [Test]
        public void AddTest()
        {
            AreEqual(set0, array0);
            AreEqual(set1, array1);
            AreEqual(set3, array3);
            AreEqual(set4, array4);
            AreEqual(set5, array5);
            AreEqual(set8, array8);
            AreEqual(set11, array11);
        }

        [Test]
        public void IsReadOnlyTest()
        {

        }


        [Test]
        public void ContainsTest()
        {
            for (int i = 0; i < array11.Length; ++i)
            {
                Assert.IsTrue(set11.Contains(array11[i]));
            }
        }

        [Test]
        public void RemoveTest()
        {
            
        }

        [Test]
        public void ClearTest()
        {
            set1.Clear();
            Assert.IsTrue(set1.Count == 0);
        }

        [Test]
        public void Overlaps()
        {
            
        }

        [Test]
        public void IntersectWith()
        {
            
        }

        [Test]
        public void ExceptWith()
        {
            
        }

        [Test]
        public void SymmetricExceptWithTest()
        {

        }

        [Test]
        public void UnionWithTest()
        {
        }

        [Test]
        public void SetEqualsTest()
        {

        }

        [Test]
        public void CorrectCopyToTest()
        {
            
        }

        [Test]
        public void InorrectCopyToTestTooShortArray()
        {

        }

        [Test]
        public void InorrectCopyToTestNonOneDimensionalArray()
        {

        }

        [Test]
        public void IncorrectCopyToTestNegativeArrayIndex()
        {

        }

        [Test]
        public void IsSubsetOfTest()
        {
            
        }

        [Test]
        public void IsProperSubsetOfTest()
        {
            
        }

        [Test]
        public void IsSupersetOfTest()
        {
            
        }

        [Test]
        public void IsProperSupersetOfTest()
        { }

    }
}
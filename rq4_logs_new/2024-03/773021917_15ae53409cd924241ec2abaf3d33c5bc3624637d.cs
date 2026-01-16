using OOPAssignment1;
using static OOPAssignment1.Set;

namespace TestSet
{
    [TestClass]
    public class Test
    {
        [TestMethod]
        public void Test_Insert()
        {
            Set s = new Set();

            //correct cases
            s.Insert(5);
            s.Insert(7);
            Assert.IsTrue(s.Contains(5));
            Assert.IsTrue(s.Contains(7));

            //error cases
            //Inserting an element that already exists in the  set
            try
            {
                s.Insert(1);
                s.Insert(1);
                Assert.Fail("No exception thrown");
            }
            catch(Exception ex)
            {
                Assert.IsTrue(ex is ElementAlreadyExists);
            }
        }


        [TestMethod]
        public void Test_Remove()
        {
            //correct cases
            Set s = new Set();
            s.Insert(1);
            s.Insert(2);
            s.Remove(1);
            Assert.IsFalse(s.Contains(1));
            Assert.IsTrue(s.Contains(2));

            //error cases
            Set s2 = new Set();
            try
            {
                
                s2.Remove(2);
                Assert.Fail("No exception thrown");
            }
            catch (Exception ex1)
            {
                //removing an element when the set is empty
                Assert.IsTrue(ex1 is SetEmpty);
            }
            try
            {
                //removing an element that does not exist in the set
                s2.Insert(2);
                s2.Remove(3);
                Assert.Fail("No exception thrown");
            }
            catch (Exception ex2)
            {
                Assert.IsTrue(ex2 is ElementDoesNotExist);
            }

        }


        [TestMethod]
        public void Test_IsEmpty()
        {
            //set is empty
            Set s = new Set();
            Assert.IsTrue(s.IsEmpty());

            //set is not empty 
            Set s2 = new Set();
                s2.Insert(6);
                s2.Insert(7);
                s2.Insert(8);
            Assert.IsFalse(s2.IsEmpty());
            
        }


        [TestMethod]
        public void Test_Contains()
        {
            //correct cases
            //when the set contains the element given by the user
            Set s = new Set();
            s.Insert(2);
            s.Insert(4);

            Assert.IsTrue(s.Contains(2));
            Assert.IsTrue(s.Contains(4));

            //when the set does not contain the element given by the user
            Assert.IsFalse(s.Contains(5));


            //exception cases
            Set s2 = new Set();
            try
            {
                s2.Contains(3); //asking if an element is part of an empty set

            }
            catch (Exception Contains)
            {
                Assert.IsTrue(Contains is SetEmpty);
            }


        }
        [TestMethod]
        public void Test_RandomElement()
        {
            //correct cases
            Set s = new Set();
            s.Insert(1);
            s.Insert(5);
            s.Insert(9);

            int randomelement = s.RandomElement();
            Assert.IsTrue(s.Contains(randomelement));

            //exception cases
            Set s2 = new Set();
            try
            {
                s2.RandomElement(); //returning a random element from an empty set
            }
            catch(Exception RandomElement)
            {
                Assert.IsTrue(RandomElement is SetEmpty);
            }
            
        }


        [TestMethod]
        public void Test_Maximum()
        {
            //correct cases
            Set s = new Set();
            s.Insert(5);
            s.Insert(21);
            s.Insert(33);

            int max = s.Maximum();
            Assert.AreEqual(33, max);

            //exception cases
            Set s2 = new Set();
            try
            {
                int max2 = s2.Maximum();
            }
            catch (Exception Maximum)
            {
                Assert.IsTrue(Maximum is SetEmpty);
            }

        }

        [TestMethod]
        public void Test_Print()
        {
            Set s = new Set();
            s.Insert(5);
            s.Insert(10);
            s.Insert(15);
            s.Insert(20);

            bool Output = s.Print();
            //string ExpectedOutPut = ("Current Set: 5,10,15,20");
            
            Assert.AreEqual(true, Output);
            
            
        }

    }
}
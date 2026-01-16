using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using ClassLibrary;
using System.Collections.Generic;

namespace Testing4
{
    [TestClass]
    public class tstOrderCollection
    {
        [TestMethod]
        public void InstanceOK()
        {
            clsOrderCollection AllOrder = new clsOrderCollection();
            // test to see if that exits 
            Assert.IsNotNull(AllOrder);

        }

        [TestMethod]
        public void OrderListOK()
        {
            //create an instance of the class we want to create
            clsOrderCollection AllOrder = new clsOrderCollection();
            //create some test data to assign to the property
            //in this case the data needs to be a list of objects
            List<clsOrder> TestList = new List<clsOrder>();
            //add an item to the list
            //create the item of test data
            clsOrder TestItem = new clsOrder();
            //set its properties
            TestItem.OrderNo = 3;
            TestItem.OrderPrice = 45.0;
            TestItem.Address = "some street, some town, 123a, LE1 4FT";
            TestItem.Dispatched = true;
            TestItem.OrderQnty = 2;
            TestItem.DateofPurchase = DateTime.Now.Date;
            //Add the item to the test list
            TestList.Add(TestItem);
            //assign the data to the property
            AllOrder.OrderList = TestList;
            //test to see that the two values are the same
            Assert.AreEqual(AllOrder.OrderList, TestList);

        }

        [TestMethod]
        public void ThisOrderPropertyOK()
        {
            //create an instance of the class we want to create
            clsOrderCollection AllOrder = new clsOrderCollection();
            //create some test data to assign to the property
            clsOrder TestOrder = new clsOrder();
            //set the properties of the test object
            TestOrder.OrderNo = 3;
            TestOrder.OrderPrice = 45.0;
            TestOrder.Address = "some street, some town, 123a, LE1 4FT";
            TestOrder.Dispatched = true;
            TestOrder.OrderQnty = 2;
            TestOrder.DateofPurchase = DateTime.Now.Date;
            //assign the data to the property
            AllOrder.ThisOrder = TestOrder;
            //test to see that the two values are the same
            Assert.AreEqual(AllOrder.ThisOrder, TestOrder);

        }
        [TestMethod]
        public void ListAndCountOK()
        {
            //create an instance of the class we want to create
            clsOrderCollection AllOrder = new clsOrderCollection();
            //create some test data to assign to the property
            // in this case the data needs to be a list of objects
            List<clsOrder> TestList = new List<clsOrder>();
            //add an item to the list
            //create the item of test data
            clsOrder TestItem = new clsOrder();
            //set the properties of the test object
            TestItem.OrderNo = 3;
            TestItem.OrderPrice = 45.0;
            TestItem.Address = "some street, some town, 123a, LE1 4FT";
            TestItem.Dispatched = true;
            TestItem.OrderQnty = 2;
            TestItem.DateofPurchase = DateTime.Now.Date;
            //add the item to the test list
            TestList.Add(TestItem);
            //assign the data to the property
            AllOrder.OrderList = TestList;
            //test to see that the two values are the same
            Assert.AreEqual(AllOrder.Count, TestList.Count);

        }
        [TestMethod]
        public void AddMethodOK()
        {
            //create an instance of the class we want to create
            clsOrderCollection AllOrder = new clsOrderCollection();
            //create the item of test data
            clsOrder TestItem = new clsOrder();
            //var to store the primary key
            Int32 PrimaryKey = 0;
            //set its properties
            TestItem.Dispatched = true;
            TestItem.Address = "109 Hollywood Avenue, Birmingham, B35 4HE";
            TestItem.DateofPurchase = DateTime.Now.Date;
            TestItem.OrderNo = 5;
            TestItem.OrderPrice = 150.00;
            TestItem.OrderQnty = 2;
            //SET ThisOrder to the test data
            AllOrder.ThisOrder = TestItem;
            //add the record
            PrimaryKey = AllOrder.Add();
            //set the primary key of the test data
            TestItem.OrderNo = PrimaryKey;
            //find the record
            AllOrder.ThisOrder.Find(PrimaryKey);
            //test to see that the two values are the same
            Assert.AreEqual(AllOrder.ThisOrder, TestItem);
        }
        [TestMethod]
        public void UpdateMethodOK()
        {
            //create an instance of the class we want to create
            clsOrderCollection AllOrder = new clsOrderCollection();
            //create the item of test data
            clsOrder TestItem = new clsOrder();
            //var to store the primary key
            Int32 PrimaryKey = 0;
            //set its properties
            TestItem.Dispatched = true;
            TestItem.Address = "109 Hollywood Avenue, Birmingham, B35 4HE";
            TestItem.DateofPurchase = DateTime.Now.Date;
            TestItem.OrderNo = 5;
            TestItem.OrderPrice = 150.00;
            TestItem.OrderQnty = 2;
            //SET ThisOrder to the test data
            AllOrder.ThisOrder = TestItem;
            //add the record
            PrimaryKey = AllOrder.Add();
            //set the primary key of the test data
            TestItem.OrderNo = PrimaryKey;
            //modify the test data
            TestItem.Dispatched = false;
            TestItem.Address = "103 Southgate Avenue, Birmingham, B25 4HE";
            TestItem.DateofPurchase = DateTime.Now.Date;
            TestItem.OrderNo = 6;
            TestItem.OrderPrice = 50.00;
            TestItem.OrderQnty = 4;
            //set the record based on the new test data
            AllOrder.ThisOrder = TestItem;
            //update the record
            AllOrder.Update();
            //find the record
            AllOrder.ThisOrder.Find(PrimaryKey);
            //test to see that the two values are the same
            Assert.AreEqual(AllOrder.ThisOrder, TestItem);
        }
        [TestMethod]
        public void DeleteMethodOK()
        {
            //create an instance of the class we want to create
            clsOrderCollection AllOrder = new clsOrderCollection();
            //create the item of test data
            clsOrder TestItem = new clsOrder();
            //var to store the primary key
            Int32 PrimaryKey = 0;
            //set its properties
            TestItem.Dispatched = true;
            TestItem.OrderNo = 7;
            TestItem.Address = "109 Hollywood Avenue, Birmingham, B35 4HE";
            TestItem.DateofPurchase = DateTime.Now.Date;
            TestItem.OrderNo = 5;
            TestItem.OrderPrice = 150.00;
            TestItem.OrderQnty = 2;
            //SET ThisOrder to the test data
            AllOrder.ThisOrder = TestItem;
            //add the record
            PrimaryKey = AllOrder.Add();
            //set the primary key of the test data
            TestItem.OrderNo = PrimaryKey;
            //find the record
            AllOrder.ThisOrder.Find(PrimaryKey);
            //delete the record
            AllOrder.Delete();
            //now find the record
            Boolean Found = AllOrder.ThisOrder.Find(PrimaryKey);
            //test to see that the two values are the same
            Assert.IsFalse(Found);
        }
        [TestMethod]
        public void ReportByAddressMethodOK()
        {
            //create an instance of the class containing unfiltered results
            clsOrderCollection AllOrder = new clsOrderCollection();
            //create an instance of the filtered data
            clsOrderCollection FilteredOrder = new clsOrderCollection();
            //apply a blank string (should return all records);
            FilteredOrder.ReportByAddress("");
            //test to see that the two values are the same
            Assert.AreEqual(AllOrder.Count, FilteredOrder.Count);

        }
        [TestMethod]
        public void ReportByAddressNoneFound()
        {
            //create an instance of the filtered data
            clsOrderCollection FilteredOrder = new clsOrderCollection();
            //apply an address that doesn't exist
            FilteredOrder.ReportByAddress("blabla blabla");
            //test to see that there are no recorders
            Assert.AreEqual(0, FilteredOrder.Count);

        }

        [TestMethod]
        public void ReportByAddressTestDataFound()
        {
            //create an instance of the filtered data
            clsOrderCollection FilteredOrder = new clsOrderCollection();
            //var to store outcome
            Boolean OK = true;
            //apply a address that doesn't exist
            FilteredOrder.ReportByAddress("yyyy");
            //check that the correct number of records are found
            if (FilteredOrder.Count == 2)
            {
                //check that the first record record is ID 8
                if (FilteredOrder.OrderList[0].OrderNo != 8)
                {
                    OK = false;
                }
                //check that the first record is ID 9
                if (FilteredOrder.OrderList[1].OrderNo != 9)
                {
                    OK = false;
                }
            }
            else
            {
                OK = false;
            }
            //test to see that there are no records
            Assert.IsTrue(OK);
        }
    }
}
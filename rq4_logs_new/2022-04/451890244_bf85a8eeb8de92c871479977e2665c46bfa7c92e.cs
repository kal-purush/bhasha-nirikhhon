using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using ClassLibrary;
using System.Collections.Generic;


namespace Testing2
{
    [TestClass]
    public class tstStockCollection
    {
        [TestMethod]
        public void ReportItemNameTestDataFound()
        {
            clsStockCollection FilteredNames = new clsStockCollection();
            Boolean OK = true;
            FilteredNames.ReportByItemName("CorsairK55");
            if (FilteredNames.Count == 2)
            {
                if (FilteredNames.StockList[0].ItemID != 1)
                {
                    OK = false;
                }
                if (FilteredNames.StockList[1].ItemID != 2)
                {
                    OK = false;
                }
            }
            else
            {
                OK = false;
            }
            Assert.IsTrue(OK);
        }

        [TestMethod]
        public void ReportByItemNameMethodOK()
        {
            clsStockCollection allstock = new clsStockCollection();
            clsStockCollection FilteredNames = new clsStockCollection();
            FilteredNames.ReportByItemName("");
            Assert.AreEqual(allstock.Count, FilteredNames.Count);
        }

        [TestMethod]
        public void ReportByItemNameNoneFound()
        {
            //create an instance of the filtered data
            clsStockCollection FilteredNames = new clsStockCollection();
            //apply a name that doesn't exist
            FilteredNames.ReportByItemName("xxxx");
            //test to see that there are no records
            Assert.AreEqual(0, FilteredNames.Count);
        }

        [TestMethod]
        public void DeleteMethodOK()
        {
            clsStockCollection ALLstock = new clsStockCollection();
            clsStock TestItem = new clsStock();
            Int32 primarykey = 0;
            TestItem.ItemName = "DeleteMethodCheck";
            TestItem.ItemOver18 = false;
            TestItem.ItemPrice = 35.00;
            TestItem.ItemQuantity = 12;
            TestItem.ItemDateAdded = DateTime.Now.Date;
            ALLstock.ThisStock = TestItem;
            primarykey = ALLstock.Add();
            TestItem.ItemID = primarykey;
            ALLstock.ThisStock.Find(primarykey);
            ALLstock.Delete();
            bool Found = ALLstock.ThisStock.Find(primarykey);
            Assert.IsFalse(Found);
        }

        [TestMethod]
        public void UpdateMethodOK()
        {
            clsStockCollection AllStock = new clsStockCollection();
            clsStock TestItem = new clsStock();
            Int32 primarykey = 0;
            TestItem.ItemName = "UpdateMethodCheck";
            TestItem.ItemOver18 = false;
            TestItem.ItemPrice = 35.00;
            TestItem.ItemQuantity = 12;
            TestItem.ItemDateAdded = DateTime.Now.Date;
            AllStock.ThisStock = TestItem;
            primarykey = AllStock.Add();
            TestItem.ItemID = primarykey;
            TestItem.ItemName = "UpdateMethodCheck2";
            TestItem.ItemOver18 = false;
            TestItem.ItemPrice = 39.99;
            TestItem.ItemQuantity = 20;
            TestItem.ItemDateAdded = DateTime.Now.Date;
            AllStock.ThisStock = TestItem;
            AllStock.Update();
            AllStock.ThisStock.Find(primarykey);
            Assert.AreEqual(AllStock.ThisStock, TestItem);
        }

        [TestMethod]
        public void AddMethodOK()
        {
            clsStockCollection AllStock = new clsStockCollection();
            clsStock TestItem = new clsStock();
            Int32 primarykey = 0;
            TestItem.ItemID = 5;
            TestItem.ItemName = "AddMethodCheck";
            TestItem.ItemOver18 = false;
            TestItem.ItemPrice = 35.00;
            TestItem.ItemQuantity = 12;
            TestItem.ItemDateAdded = DateTime.Now.Date;
            AllStock.ThisStock = TestItem;
            primarykey = AllStock.Add();
            TestItem.ItemID = primarykey;
            AllStock.ThisStock.Find(primarykey);
            Assert.AreEqual(AllStock.ThisStock, TestItem);
        }

        [TestMethod]
        public void InstanceOK()
        {
            clsStockCollection AllStock = new clsStockCollection();
            Assert.IsNotNull(AllStock);
        }

        [TestMethod]
        public void StockListOK()
        {
            clsStockCollection AllStock = new clsStockCollection();
            List<clsStock> TestList = new List<clsStock>();
            clsStock TestItem = new clsStock();
            TestItem.ItemID = 5;
            TestItem.ItemName = "StockListOKCheck";
            TestItem.ItemOver18 = false;
            TestItem.ItemPrice = 35.00;
            TestItem.ItemQuantity = 12;
            TestItem.ItemDateAdded = DateTime.Now.Date;
            TestList.Add(TestItem);
            AllStock.StockList = TestList;
            Assert.AreEqual(AllStock.StockList, TestList);
        }

        [TestMethod]
        public void ThisStockPropertyOK()
        {
            clsStockCollection AllStock = new clsStockCollection();
            clsStock TestItem = new clsStock();
            TestItem.ItemID = 5;
            TestItem.ItemName = "ThisStockCheck";
            TestItem.ItemOver18 = false;
            TestItem.ItemPrice = 35.00;
            TestItem.ItemQuantity = 12;
            TestItem.ItemDateAdded = DateTime.Now.Date;
            AllStock.ThisStock = TestItem;
            Assert.AreEqual(AllStock.ThisStock, TestItem);
        }

        [TestMethod]
        public void ListAndCountOK()
        {
            clsStockCollection AllStock = new clsStockCollection();
            List<clsStock> TestList = new List<clsStock>();
            clsStock TestItem = new clsStock();
            TestItem.ItemID = 5;
            TestItem.ItemName = "ListAndCountCheck";
            TestItem.ItemOver18 = false;
            TestItem.ItemPrice = 35.00;
            TestItem.ItemQuantity = 12;
            TestItem.ItemDateAdded = DateTime.Now.Date;
            TestList.Add(TestItem);
            AllStock.StockList = TestList;
            Assert.AreEqual(AllStock.Count, TestList.Count);
        }
    }
}
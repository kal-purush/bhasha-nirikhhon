using System;
using System.Collections.Generic;
using System.Linq;
using Toolkit.Syncable;
using Xunit;

namespace Tests.Syncable
{
    public class SyncableObjectCollectionTests
    {
        private class TestObject : SyncableObject
        {
            public string Name { get; set; }
        }

        private readonly SyncableObjectCollection<TestObject> _testObject;

        public SyncableObjectCollectionTests()
        {
            _testObject = new SyncableObjectCollection<TestObject>();
        }

        [Fact]
        public void AssertIsEmptyForEmptyCollection()
        {
            Assert.True(_testObject.IsEmpty);
        }

        [Fact]
        public void AssertIsEmptyForNonEmptyCollection()
        {
            _testObject.Add(new TestObject());
            Assert.False(_testObject.IsEmpty);
        }

        [Fact]
        public void AssertCountForRandomAddsAndRemoves()
        {
            var addedItems = new List<TestObject>();
            var itemCountAdded = new Random().Next(100);
            var itemCountRemoved = new Random().Next(itemCountAdded);
            var expectedItemCount = itemCountAdded - itemCountRemoved;

            for (int i = 0; i < itemCountAdded; i++)
            {
                var item = new TestObject();
                _testObject.Add(item);
                addedItems.Add(item);
            }

            foreach (var item in addedItems.Take(itemCountRemoved))
            {
                _testObject.Remove(item);
            }

            Assert.Equal(expectedItemCount, _testObject.Count);
        }

        [Fact]
        public void AssertCountForAdd()
        {
            _testObject.Add(new TestObject());
            _testObject.Add(new TestObject());

            Assert.Equal(2, _testObject.Count);
        }

        [Fact]
        public void AssertRemoveForTwoAddsAndOneRemove()
        {
            var item_one = new TestObject();
            var item_two = new TestObject();

            _testObject.Add(item_one);
            _testObject.Add(item_two);

            _testObject.Remove(item_two);

            Assert.Single(_testObject);
        }

        [Fact]
        public void AssertRemoveTrueOnSuccess()
        {
            var item = new TestObject();

            _testObject.Add(item);
            var result = _testObject.Remove(item);

            Assert.True(result);
        }

        [Fact]
        public void AssertRemoveFalseOnFail()
        {
            var item_one = new TestObject();
            var item_two = new TestObject();

            _testObject.Add(item_one);
            var result = _testObject.Remove(item_two);

            Assert.False(result);
        }

        [Fact]
        public void AssertAddArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => _testObject.Add(null));
        }

        [Fact]
        public void AssertRemoveArgumentNullException()
        {
            Assert.Throws<ArgumentNullException>(() => _testObject.Remove(null));
        }

        [Fact]
        public void AssertAddItemAddedIsSelected()
        {
            var item = new TestObject();
            _testObject.Add(item);
            Assert.Equal(item, _testObject.SelectedItem);
        }

        [Fact]
        public void AssertIsReadonlyFalse()
        {
            Assert.False(_testObject.IsReadOnly);
        }

        [Fact]
        public void AssertCollectionViewNotNull()
        {
            Assert.NotNull(_testObject.CollectionView);
        }

        [Fact]
        public void AssertFilterNotNull()
        {
            Assert.NotNull(_testObject.CollectionView.Filter);
        }

        [Fact]
        public void AssertCollectionNotFiltered()
        {
            _testObject.Add(new TestObject());
            _testObject.Add(new TestObject());
            _testObject.Add(new TestObject());
            _testObject.Add(new TestObject());

            var count = _testObject.CollectionView.Cast<object>().Count();
            Assert.Equal(4, count);
        }

        [Fact]
        public void AssertCollectionFiltered()
        {

            _testObject.Add(new TestObject() { Name = "MyName" });
            _testObject.Add(new TestObject() { Name = "YourName" });
            _testObject.Add(new TestObject() { Name = "123" });
            _testObject.Add(new TestObject() { Name = "3131" });

            _testObject.CollectionViewFilter = (item) => item.Name.IndexOf(_testObject.FilterText, StringComparison.OrdinalIgnoreCase) >= 0;
            _testObject.FilterText = "Name";

            Assert.Equal(2, _testObject.CollectionView.Cast<object>().Count());

            _testObject.FilterText = "";

            Assert.Equal(4, _testObject.CollectionView.Cast<object>().Count());
        }
    }
}
using System.Collections.Generic;
using System.Linq;

using Microsoft.VisualStudio.TestTools.UnitTesting;

using Moq;

using PicnicOrm.Dapper.Mapping;

namespace PicnicOrm.Dapper.UnitTests.Mapping
{
    [TestClass]
    public class ManyToManyMappingShould
    {
        #region Properties

        public Mock<IChildMapping<ManyToManyItem>> MockChildMapping { get; set; }

        public Mock<IGridReader> MockGridReader { get; set; }

        public ManyToManyMapping<ParentItem, ManyToManyItem, ManyToManyLinker> ManyToManyMapping { get; set; }

        #endregion

        #region Public Methods

        [TestInitialize]
        public void Initialize()
        {
            MockChildMapping = new Mock<IChildMapping<ManyToManyItem>>();
            MockGridReader = new Mock<IGridReader>();

            ManyToManyMapping = new ManyToManyMapping<ParentItem, ManyToManyItem, ManyToManyLinker>(child => child.Id,
                linker => linker.ChildId,
                linker => linker.ParentId,
                (parent, children) => parent.ManyToManyChildren = children.ToList());
        }

        #endregion

        #region Test Methods

        [TestMethod]
        public void Map_HasNoResultsAndShouldContinueThroughEmptyTablesIsFalse_DoesNotMapChildren()
        {
            //Arrange
            var manyToManyItemList = new List<ManyToManyItem>();
            MockGridReader.Setup(gridReader => gridReader.Read<ManyToManyItem>()).Returns(manyToManyItemList);
            ManyToManyMapping.AddMapping(MockChildMapping.Object);
            IDictionary<int, ParentItem> parents = null;

            //Act
            ManyToManyMapping.Map(MockGridReader.Object, parents, false);

            //Assert
            MockChildMapping.Verify(childMapping => childMapping.Map(It.IsAny<IGridReader>(), It.IsAny<IDictionary<int, ManyToManyItem>>(), It.IsAny<bool>()), Times.Never);
        }

        [TestMethod]
        public void Map_HasNoResultsAndShouldContinueThroughEmptyTablesIsTrue_MapsChildren()
        {
            //Arrange
            var manyToManyItemList = new List<ManyToManyItem>();
            MockGridReader.Setup(gridReader => gridReader.Read<ManyToManyItem>()).Returns(manyToManyItemList);
            ManyToManyMapping.AddMapping(MockChildMapping.Object);
            IDictionary<int, ParentItem> parents = null;

            //Act
            ManyToManyMapping.Map(MockGridReader.Object, parents, true);

            //Assert
            MockChildMapping.Verify(childMapping => childMapping.Map(It.IsAny<IGridReader>(), It.IsAny<IDictionary<int, ManyToManyItem>>(), It.IsAny<bool>()), Times.Once);
        }

        [TestMethod]
        public void Map_HasResultsAndShouldContinueThroughEmptyTablesIsFalse_MapsChildren()
        {
            //Arrange
            var manyToManyItem = new ManyToManyItem();
            var manyToManyItemList = new List<ManyToManyItem> { manyToManyItem };
            MockGridReader.Setup(gridReader => gridReader.Read<ManyToManyItem>()).Returns(manyToManyItemList);
            var manyToManyLinker = new ManyToManyLinker();
            var manyToManyLinkerList = new List<ManyToManyLinker> { manyToManyLinker };
            MockGridReader.Setup(gridReader => gridReader.Read<ManyToManyLinker>()).Returns(manyToManyLinkerList);
            ManyToManyMapping.AddMapping(MockChildMapping.Object);
            IDictionary<int, ParentItem> parents = null;

            //Act
            ManyToManyMapping.Map(MockGridReader.Object, parents, false);

            //Assert
            MockChildMapping.Verify(childMapping => childMapping.Map(It.IsAny<IGridReader>(), It.IsAny<IDictionary<int, ManyToManyItem>>(), It.IsAny<bool>()), Times.Once);
        }

        [TestMethod]
        public void Map_HasResultsAndParents_MapsParents()
        {
            //Arrange
            var manyToManyItem = new ManyToManyItem { Id = 5 };
            var manyToManyItem2 = new ManyToManyItem { Id = 8 };
            var manyToManyItem3 = new ManyToManyItem { Id = 13 };
            var manyToManyItemList = new List<ManyToManyItem> { manyToManyItem, manyToManyItem2, manyToManyItem3 };
            MockGridReader.Setup(gridReader => gridReader.Read<ManyToManyItem>()).Returns(manyToManyItemList);
            var parent = new ParentItem { Id = 1 };
            var parent2 = new ParentItem { Id = 2 };
            var linker = new ManyToManyLinker { ChildId = 5, ParentId = 1 };
            var linker2 = new ManyToManyLinker { ChildId = 13, ParentId = 1 };
            var linker3 = new ManyToManyLinker { ChildId = 8, ParentId = 2 };
            var linker4 = new ManyToManyLinker { ChildId = 13, ParentId = 2 };
            var linkerList = new List<ManyToManyLinker> { linker, linker2, linker3, linker4 };
            MockGridReader.Setup(gridReader => gridReader.Read<ManyToManyLinker>()).Returns(linkerList);
            var parentDictionary = new Dictionary<int, ParentItem> { [1] = parent, [2] = parent2 };

            //Act
            ManyToManyMapping.Map(MockGridReader.Object, parentDictionary, true);

            //Assert
            Assert.AreEqual(2, parentDictionary[1].ManyToManyChildren.Count);
            Assert.IsTrue(parentDictionary[1].ManyToManyChildren.Contains(manyToManyItem));
            Assert.IsTrue(parentDictionary[1].ManyToManyChildren.Contains(manyToManyItem3));

            Assert.AreEqual(2, parentDictionary[2].ManyToManyChildren.Count);
            Assert.IsTrue(parentDictionary[2].ManyToManyChildren.Contains(manyToManyItem2));
            Assert.IsTrue(parentDictionary[2].ManyToManyChildren.Contains(manyToManyItem3));
        }

        #endregion
    }
}
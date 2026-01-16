using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Moq;
using SsisComponents.Base.Adapters.Abstract;
using SsisComponents.Base.CustomProperties.Abstract;
using SsisComponents.Base.CustomProperties.Concrete;
using SsisComponents.Transformations.Components.Concrete;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace SsisComponents.Tests.UnitTests
{
    public class TrimAndToUpperComponentTests
    {
        private readonly Mock<IComponentMetadataAdapter> _metaDataAdapter;
        private readonly Mock<IComComponentAdapter> _baseComponentComAdapter;

        public TrimAndToUpperComponentTests()
        {
            _metaDataAdapter = new Mock<IComponentMetadataAdapter>();
            _baseComponentComAdapter = new Mock<IComComponentAdapter>();
        }

        [Fact]
        public void TrimAndToUpperComponent_ProvideComponentProperties_Succeeds()
        {
            var component = new TrimAndToUpperComponent(_metaDataAdapter.Object, _baseComponentComAdapter.Object);

            component.ProvideComponentProperties();

            _metaDataAdapter
                .Verify(d => d.AddNewCustomDesignTimeProperty(It.IsAny<ICustomPropertyBuilder>()), Times.Exactly(2));

            _metaDataAdapter
                .Verify(d => d.AddNewCustomDesignTimeProperty(It.Is<ICustomPropertyBuilder>(c =>
                    c.PropertyName == "Trim" &&
                    c.PropertyDescription == "Indicates whether or not to Trim() the column data" &&
                    (bool)c.Value)),
                Times.Once());

            _metaDataAdapter
                .Verify(d => d.AddNewCustomDesignTimeProperty(It.Is<ICustomPropertyBuilder>(c =>
                    c.PropertyName == "Uppercase" &&
                    c.PropertyDescription == "Indicates whether or not ToUpper() should be performed on the column data" &&
                    (bool)c.Value)),
                Times.Once());
        }
    }
}
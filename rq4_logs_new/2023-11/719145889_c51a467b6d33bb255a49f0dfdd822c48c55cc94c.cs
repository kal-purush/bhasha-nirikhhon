namespace legallead.resources.tests
{
    public class ResourceTableTests
    {

        [Theory]
        [InlineData(ResourceType.Xml)]
        [InlineData(ResourceType.FindDefendant)]
        [InlineData(ResourceType.ParameterName)]
        [InlineData(ResourceType.ActionName)]
        [InlineData(ResourceType.FormatString)]
        [InlineData(ResourceType.JscriptCommand)]
        public void ResourceTableCanGetResourceByType(ResourceType resourceType)
        {
            var response = ResourceTable.GetResources(resourceType);
            Assert.NotEmpty(response);
        }
    }
}
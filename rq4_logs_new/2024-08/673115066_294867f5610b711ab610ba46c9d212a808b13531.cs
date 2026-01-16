namespace AI21.IntegrationTests;

public partial class Tests
{
    [TestMethod]
    public void Generate()
    {
        using var api = GetAuthorizedApi();
    }
}
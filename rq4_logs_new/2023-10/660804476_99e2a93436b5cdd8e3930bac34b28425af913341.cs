using graph_business_trip;

namespace TestTrip
{
    public class UnitTest1
    {
        [Fact]
        public void Test1()
        {

        }
        [Fact]
        public void TestValidTrip()
        {
            Dictionary<string, Dictionary<string, int>> graph = new Dictionary<string, Dictionary<string, int>>()
        {
            {"Metroville", new Dictionary<string, int>() {{"Pandora", 82}, {"Narnia", 37}, { "Naboo", 26},{"Arendelle", 99},{"New Monstropolis", 105}}},
            {"Pandora", new Dictionary<string, int>() {{"Metroville", 82}, {"Arendelle", 150}}},
            {"Arendelle", new Dictionary<string, int>() {{"Pandora", 150}, {"New Monstropolis", 42}, {"Naboo", 26}}},
            {"New Monstropolis", new Dictionary<string, int>() {{"Arendelle", 42}, { "Naboo", 73}}},
            {"Naboo", new Dictionary<string, int>() {{"Arendelle", 26},{"Metroville", 73}, {"Narnia", 250}}},
            {"Narnia", new Dictionary<string, int>() {{"Metroville", 37}, {"Naboo", 250}}}
        }; string[] cityNames1 = { "Metroville", "Pandora" };

            // Act
            int? tripCost = BusinessTrip.BusinesTrip(graph, cityNames1);

            // Assert
            Assert.Equal(82, tripCost); // Expected trip cost
        }

        [Fact]
        public void TestInvalidTrip()
        {
            Dictionary<string, Dictionary<string, int>> graph = new Dictionary<string, Dictionary<string, int>>()
        {
            {"Metroville", new Dictionary<string, int>() {{"Pandora", 82}, {"Narnia", 37}, { "Naboo", 26},{"Arendelle", 99},{"New Monstropolis", 105}}},
            {"Pandora", new Dictionary<string, int>() {{"Metroville", 82}, {"Arendelle", 150}}},
            {"Arendelle", new Dictionary<string, int>() {{"Pandora", 150}, {"New Monstropolis", 42}, {"Naboo", 26}}},
            {"New Monstropolis", new Dictionary<string, int>() {{"Arendelle", 42}, { "Naboo", 73}}},
            {"Naboo", new Dictionary<string, int>() {{"Arendelle", 26},{"Metroville", 73}, {"Narnia", 250}}},
            {"Narnia", new Dictionary<string, int>() {{"Metroville", 37}, {"Naboo", 250}}}
        }; 

            string[] cityNames = { "New Monstropolis", "Narnia" };

            // Act
            int? tripCost = BusinessTrip.BusinesTrip(graph, cityNames);

            // Assert
            Assert.Null(tripCost); // Trip is not possible
        }

    }
}
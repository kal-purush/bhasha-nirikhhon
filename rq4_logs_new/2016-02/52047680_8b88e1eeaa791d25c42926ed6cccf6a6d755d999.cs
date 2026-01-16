using Denormaleezie;
using Denormaleezie.Tests.Test_Classes;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Dnz.E2E
{
    public class When_Calling_DenormalizeToJSON_With_A_Null_Object
    {
        private string json;

        public When_Calling_DenormalizeToJSON_With_A_Null_Object()
        {
            Denormalizer denormalizer = new Denormalizer();

            json = denormalizer.DenormalizeToJSON<object>(null);
        }

        [Fact]
        public void It_Should_Return_An_Empty_String()
        {
            Assert.Equal(string.Empty, json);
        }
    }
    public class When_Calling_DenormalizeToJSON_With_A_List_Of_Flat_Objects
    {
        private string json;

        public When_Calling_DenormalizeToJSON_With_A_List_Of_Flat_Objects()
        {
            Denormalizer denormalizer = new Denormalizer();

            List<Animal> animals = new List<Animal>()
            {
                new Animal() {AnimalId = 1, Age = 10, Name = "Tony", Type = "Tiger" },
                new Animal() {AnimalId = 2, Age = 11, Name = "Lenny", Type = "Tiger" },
                new Animal() {AnimalId = 3, Age = 2, Name = "John", Type = "Tiger" },
                new Animal() {AnimalId = 4, Age = 15, Name = "Tony", Type = "Giraffe" },
                new Animal() {AnimalId = 5, Age = 10, Name = "Garry", Type = "Giraffe" },
                new Animal() {AnimalId = 6, Age = 10, Name = "Zachary", Type = "Zebra" },
            };

            json = denormalizer.DenormalizeToJSON(animals);
        }
        
        [Fact]
        public void It_Should_Return_JSON()
        {
            JToken.Parse(json);
        }
    }
}
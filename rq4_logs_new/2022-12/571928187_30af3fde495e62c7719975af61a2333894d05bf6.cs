namespace TrainingGround.Tests
{
    public class CollectionTests
    {
        
        [Fact]
        public void an_array_has_a_length()
        { 
            
            // act
            var arrayOfIntegers = new int[5];
            var arrayofIntegersWithValues = new int[] { 1, 2, 3};
            var arrayOfStrings = new string[] { "Name 1", "Name 2" };

            // assert
            Assert.Equal(5, arrayOfIntegers.Length);
            Assert.Equal(3, arrayofIntegersWithValues.Length);
            Assert.Equal(2, arrayOfStrings.Length);

        }
    
    }
}
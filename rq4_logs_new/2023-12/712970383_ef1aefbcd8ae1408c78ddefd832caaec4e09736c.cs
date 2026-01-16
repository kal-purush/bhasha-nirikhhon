using LeetCodeTraining.SwapNodesInPairs;

namespace LeetCodeTraining.Tests
{
    public class SwapNodesInPairsTests
    {
        [Theory]
        [MemberData(nameof(Data))]
        public void SwapPairsTests(ListNode node, ListNode expectedResult)
        {
            //Act
            ListNode result = SwapNodesInPairsSolution.SwapPairs(node);

            //Assert
            Assert.Equal(expectedResult, result);
        }

        public static IEnumerable<object[]> Data =>
            new List<object[]>
            {
                new object[]
                {
                    new ListNode(new int[] { 1, 2, 3, 4 }),
                    new ListNode(new int[] { 2, 1, 4, 3 })
                },
                new object[]
                {
                    null,
                    null
                },
                new object[]
                {
                    new ListNode(new int[] { 1 }),
                    new ListNode(new int[] { 1 })
                },
                new object[]
                {
                    new ListNode(new int[] { 1, 2 }),
                    new ListNode(new int[] { 2, 1 })
                },
                new object[]
                {
                    new ListNode(new int[] { 1, 2, 3 }),
                    new ListNode(new int[] { 2, 1, 3 })
                },
                new object[]
                {
                    new ListNode(new int[] { 1, 2, 3, 4, 5, 6, 7 }),
                    new ListNode(new int[] { 2, 1, 4, 3, 6, 5, 7 })
                }
            };
    }
}
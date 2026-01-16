using LeetCodeTraining.ReverseNodesInKGroups;

namespace LeetCodeTraining.Tests
{
    public class ReverseNodesInKGroupsTests
    {
        [Theory]
        [MemberData(nameof(Data))]
        public void ReverseKGroup(ListNode node, int k, ListNode expectedResult)
        {
            //Act
            ListNode result = ReverseNodesInKGroupsSolution.ReverseKGroup(node, k);

            //Assert
            Assert.Equal(expectedResult, result);
        }

        public static IEnumerable<object[]> Data =>
            new List<object[]>
            {
                new object[]
                {
                    new ListNode(new int[] { 1, 2, 3, 4, 5 }),
                    3,
                    new ListNode(new int[] { 3, 2, 1, 4, 5 })
                },
                new object[]
                {
                    null,
                    2,
                    null
                },
                new object[]
                {
                    new ListNode(new int[] { 1 }),
                    5,
                    new ListNode(new int[] { 1 })
                },
                new object[]
                {
                    new ListNode(new int[] { 1, 2 }),
                    2,
                    new ListNode(new int[] { 2, 1 })
                },
                new object[]
                {
                    new ListNode(new int[] { 1, 2, 3, 4 }),
                    10,
                    new ListNode(new int[] { 1, 2, 3, 4 })
                },
                new object[]
                {
                    new ListNode(new int[] { 1, 2, 3 }),
                    1,
                    new ListNode(new int[] { 1, 2, 3 })
                },
                new object[]
                {
                    new ListNode(new int[] { 1, 2, 3, 4, 5, 6, 7 }),
                    6,
                    new ListNode(new int[] { 6, 5, 4, 3, 2, 1, 7 })
                }
            };
    }
}
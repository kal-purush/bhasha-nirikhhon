/*
https://leetcode.com/problems/partition-labels/
763. Partition Labels

You are given a string s. We want to partition the string into as many parts as possible so that each letter appears in at most one part.

Note that the partition is done so that after concatenating all the parts in order, the resultant string should be s.

Return a list of integers representing the size of these parts.

Example 1:
Input: s = "ababcbacadefegdehijhklij"
Output: [9,7,8]
Explanation:
The partition is "ababcbaca", "defegde", "hijhklij".
This is a partition so that each letter appears in at most one part.
A partition like "ababcbacadefegde", "hijhklij" is incorrect, because it splits s into less parts.

Example 2:
Input: s = "eccbbbbdec"
Output: [10]

Constraints:
1 <= s.length <= 500
s consists of lowercase English letters.
*/
// 01234567890123456789
// ababcbacadefegdehijhklij
// ^       ^
//         ^
function partitionLabels(s: string): number[] {
  let partitions: number[] = [];

  let lastIndeces: { [k: string]: number } = {};
  for (let i = 0; i < s.length; i++) {
    lastIndeces[s[i]] = i;
  }

  let partitionStart = 0;
  while (partitionStart < s.length) {
    let partitionEnd = partitionStart + 1;

    for (let i = partitionStart; i < partitionEnd; i++) {
      let lastIndex = lastIndeces[s[i]];

      partitionEnd = Math.max(partitionEnd, lastIndex + 1);
    }

    partitions.push(partitionEnd - partitionStart);
    partitionStart = partitionEnd;
  }

  return partitions;
}

console.log(partitionLabels("ababcbacadefegdehijhklij"));
import operator
class Solution(object):
    def frequencySort(self, s):
        """
        :type s: str
        :rtype: str
        """
        alphaDict = {}
        for i in set(s):
            alphaDict[i] = [0, i]
        for j in s:
            alphaDict[j][0] += 1
        chint = alphaDict.values()
        sortedlist = sorted(chint, key=operator.itemgetter(0), reverse=True)
        listoutput = [None, ]*len(s)
        k = 0
        for i in sortedlist:
            for c in range(i[0]):
                listoutput[k] = i[1]
                k += 1
        return "".join(listoutput)

if __name__ == "__main__":
    s = Solution()
    g = s.frequencySort("2a554442f544asfasssffffasss")
    print(g)
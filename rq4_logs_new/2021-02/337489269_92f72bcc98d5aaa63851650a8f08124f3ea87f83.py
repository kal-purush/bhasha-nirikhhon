class Solution(object):
    
    def common_str(self,str1,str2):
        str1=Counter(str1)
        str2=Counter(str2)
        cc=str1 & str2
        list_cc=list(cc.elements())
        list_cc=sorted(list_cc)
        ans=[]
        for i in list_cc:
            ans.append(i)
        return ans

    def commonChars(self, L):
        """
        :type A: List[str]
        :rtype: List[str]"""

        n=L.__len__();
       # L=[]
        
        ans=L[0]
        for i in range(1,n):
            ans=Solution().common_str(L[i],ans)

        return ans
        
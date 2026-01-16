class Solution(object):
    def rearrangeString(self, s, k):
        """
        :type s: str
        :type k: int
        :rtype: str
        """     
        # Special case, k = 0 does not make sense, 
        # and k - 1 window is invalid
        if k == 0:
            return s

        cnt_map = {}
        for ch in s:
            cnt_map[ch] = cnt_map.get(ch, 0) + 1

        res = ["_"] * len(s)
        bag = set()
        for i in range(len(res)):
            # print i, bag, res, cnt_map
            # Here if we look at the window before s[i]
            # which is no longer than lengh K-1 (NOT K)
            # Since all same chars are at least k apart
            # the window CANNOT contains duplicates
            # So good to use a set to represent
            max_cnt = 0
            for ch in cnt_map:
                if ch not in bag and cnt_map[ch] > max_cnt:
                    max_cnt = cnt_map[ch]
                    max_cnt_ch = ch
            
            # No available options here
            if max_cnt == 0:
                return ""
            
            res[i] = max_cnt_ch 
            cnt_map[max_cnt_ch] -= 1
            bag.add(max_cnt_ch)
            
            if i - (k - 1) >= 0:
                bag.remove(res[i - (k - 1)])
        
        return "".join(res)
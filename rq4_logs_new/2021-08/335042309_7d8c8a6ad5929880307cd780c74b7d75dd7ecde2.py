def container_max(height):

    n = len(height)
    if n==2:
        return min(height)

    l = 0
    r = n-1
    curr_short = min(height[l], height[r])
    curr_vol = (r-l)*curr_short

    while l<=r:
        curr_short = min(height[r], height[l])
        curr_vol = max(curr_vol,(r-l)*curr_short)  
            
        if height[l]>height[r]:
            r -= 1
        elif height[l]<height[r]:
            l += 1
        else:
            l += 1
            r -= 1

    return curr_vol

test1 = [1,1]
print(container_max(test1))

test2 = [4,3,2,1,4]
print(container_max(test2))

test3 = [1,2,1]
print(container_max(test3))

test4 = [3,9,3,4,7,2,12,6]
print(container_max(test4))

test5 = [4,4,4,4]
print(container_max(test5))

test6 = [1,2,7,7,2,1]
print(container_max(test6))
            
test7 = [1,8,6,2,5,4,8,3,7]
print(container_max(test7))
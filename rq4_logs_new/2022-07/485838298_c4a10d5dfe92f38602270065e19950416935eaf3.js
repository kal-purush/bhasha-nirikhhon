/**
 * @param {number[]} height
 * @return {number}
 */
var maxArea = function(height) {
    //left pointer : from 0
    //right pointer : from height.length - 1
    //result = Math.max(height[left],height[right])
    //Math.min(height[left],height[right]) -> 한칸 가운데쪽으로 이동
    //분기
    
    let result = 0
    let left = 0
    let right = height.length -1
    let x,y
    
    while(left < right) {
        x = right - left;
        y = Math.min(height[left], height[right])
        result = Math.max(result, x * y)
        
        if(height[left] <= height[right]) {
            left += 1;
        } else {
            right -=1;
        }
    }
    
    return result
    
    
    
    
//     //brute force -> Time Limit Exceeded
//     //double for loop
//     //y-axis : minimum among left and right
//     //x-axis : value = right - left
//     //result = y-axis * x-axis
//     //compare finalResult < result -> finalResult = result
    
//     let finalResult = 0;
//     let result = 0;
//     let x = 0;
//     let y = 0;
//     for(let i = 0; i<height.length; i++) {
//         for(let j = i+1; j<height.length; j++) {
//             //i : left line
//             //j : right line
//             x = j - i;
//             y = Math.min(height[i], height[j])
//             result = x * y
//             finalResult = Math.max(finalResult, result)
//         }
//     }
//     return finalResult;
    
};
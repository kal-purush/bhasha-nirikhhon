const nums = [1, -2, 3, -4, 4, 4, 4, 4, 5, 6, 7, 8, 9];
for (let i = 0; i < nums.length; i++) {
    const element = nums[i]; 

    //to make a break

    // if (nums[i]>4){
    //     break;
    // }
    // console.log(element);

    //to continue by skipping
    
    if (nums[i]<0){
        continue;
    }
    console.log(element);
}
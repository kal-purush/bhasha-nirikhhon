var getConcatenation = function (nums) {
    let res = [];
    for (let i = 0; i < nums.length * 2; i++) {
      res.push(nums[i % nums.length]);
      console.log()
    }
    return res;
  };
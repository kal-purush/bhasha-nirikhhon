class MinHeap {
    constructor() {
      this.arr = [];
    }
    swap(i, j) {
      let temp = this.arr[i];
      this.arr[i] = this.arr[j];
      this.arr[j] = temp;
    }
    insert(element) {
      this.arr.push(element);
      let currIndex = this.arr.length - 1;
      while (currIndex > 0) {
        let parentIndex = Math.floor((currIndex - 1) / 2);
        if (this.arr[parentIndex] > this.arr[currIndex]) {
          this.swap(parentIndex, currIndex);
          currIndex = parentIndex;
          parentIndex = Math.floor((currIndex - 1) / 2);
        } else {
          break;
        }
      }
    }
    remove() {
      if (this.arr.length === 0) {
        return -1;
      }
      const top = this.arr[0];
      this.arr[0] = this.arr[this.arr.length - 1];
      this.arr.pop();
      let currIndex = 0;
      let totalSize = this.arr.length;
      while (currIndex < totalSize) {
        let leftChild = 2 * currIndex + 1;
        let rightChild = 2 * currIndex + 2;
        if (totalSize <= leftChild) {
          break;
        } else if (totalSize > leftChild && totalSize <= rightChild) {
          if (this.arr[currIndex] > this.arr[leftChild]) {
            this.swap(currIndex, leftChild);
          }
          break;
        } else {
          if (this.arr[leftChild] <= this.arr[rightChild]) {
            if (this.arr[currIndex] > this.arr[leftChild]) {
              this.swap(currIndex, leftChild);
              currIndex = leftChild;
            } else {
              break;
            }
          } else {
            if (this.arr[currIndex] > this.arr[rightChild]) {
              this.swap(currIndex, rightChild);
              currIndex = rightChild;
            } else {
              break;
            }
          }
        }
      }
      return top;
    }
    printHeap() {
      return this.arr;
    }
  }
var findKthLargest = function(nums, k) {
    let heap = new MinHeap()
    for(let i=0;i<nums.length;i++){
        heap.insert(nums[i])
    }
    let count = nums.length - k +1
    while(count>1){
        let val = heap.remove()
        console.log("val  ",val)
        count -= 1
    }
    return heap.remove()
};
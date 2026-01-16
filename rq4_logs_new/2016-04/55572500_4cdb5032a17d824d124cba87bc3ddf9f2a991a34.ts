export class Utils {
  constructor() {
    console.log("Utils class loaded");
  }
  
  public swap(a: any, b: any) {    
    return [b, a];
  }
  
  public calculate(func: any, arr: number[]) {
    let start = performance.now();
    func(arr);
    let end = performance.now();
    
    return end-start;
  }
  
  public generateHTML(arr: any[], type: string) {
    switch (type) {
      case 'ul':
        let $ul = document.createElement('ul');
      
        for (let i = 0; i < arr.length; i++) {
          let $li = document.createElement('li');
          
          $li.textContent = arr[i];
          $ul.appendChild($li);          
        }
        
        return $ul;
    
      default:
        break;
    }
  }
  
  public siftDown(numbers: number[], root: number, bottom: number) {
    let maxChild = root * 2 + 1;
    
    if (maxChild < bottom) {
      let otherChild = maxChild + 1;
      maxChild = (numbers[otherChild] > numbers[maxChild]) ? otherChild : maxChild;
    }
    else if (maxChild > bottom) return;
    
    if (numbers[root] >= numbers[maxChild]) return;
    
    [numbers[root], numbers[maxChild]] = this.swap(numbers[root], numbers[maxChild]);
    
    this.siftDown(numbers, maxChild, bottom);
  }
  
  public merge(numbers: number[], begin: number, mid: number, end: number) {    
    let temp: number[] = [],
        i = begin, j = mid, k = 0;
    
    while (i < mid && j < end) {
      if (numbers[i] <= numbers[j]) temp[k++] = numbers[i++];
      else temp[k++] = numbers[j++];
    }
    
    while (i < mid) temp[k++] = numbers[i++];
    while (j < end) temp[k++] = numbers[j++];
    
    for (i = begin; i < end; i++) {
      numbers[i] = temp[i-begin];
    }
    
    return numbers;
  }
  
  /** Partition method */
  public partition(numbers: number[], left: number, right: number) {
    // let pivot: number = Math.floor(left+right / 2),
    let pivot: number = left,
        i = left, j = right;       
        
    while (i <= j) {
      while (numbers[i] < numbers[pivot]) i++;
      while (numbers[j] > numbers[pivot]) j--;
      
      if (i <= j) {
        [numbers[i], numbers[j]] = this.swap(numbers[i], numbers[j]);
        
        i++;
        j--;
      }
    }
    
    return i;
  }
}
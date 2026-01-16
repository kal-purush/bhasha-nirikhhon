class ArrayClassMethods {
  constructor(value) {
    this.storage = value;
  }

  indexOf(target) {
    if (!this.storage.length) return -1
    for (let i in this.storage) {
      if (target === this.storage[i]) return i
    }
    return -1;
  }

  evens() {
    const newArr = []
    this.storage.forEach((number) => {
     if (number % 2 === 0) newArr.push(number)
    });
    return newArr
  }

  splitString(input) {
    return this.storage = this.storage.concat(input.split(''))
  }

}
  const testCollectionInt = new ArrayClassMethods([2, 5, 1, 6, 8]);
  const testCollectionString = 'howdydooru';
  const testCollectionMix = new ArrayClassMethods([2, 5, 1, 6, 8, 'hi', 'hello']);
  const testCollectionEmpty = new ArrayClassMethods([]);
  
  console.log('testCollectionMix.indexOf(6) (╯°□°)╯︵ ┻━┻ ', testCollectionMix.indexOf(6))
  console.log('testCollectionMix.indexOf(5) (╯°□°)╯︵ ┻━┻ ', testCollectionMix.indexOf('hello'))
  console.log('testCollectionMix.indexOf(22) (╯°□°)╯︵ ┻━┻ ', testCollectionMix.indexOf(22))

  console.log('testCollectionInt.evens() (╯°□°)╯︵ ┻━┻ ', testCollectionInt.evens())

  console.log('testCollectionEmpty.splitString() (╯°□°)╯︵ ┻━┻ ', testCollectionEmpty.splitString(testCollectionString))
import { Node } from './node';

export class Trie {
  public root: Node;
  constructor() {
    this.root = new Node(''); // Root node
  }

  // Function to get the index of character 't'
  public getIndex(t: string): number {
    return t.charCodeAt(0) - 'a'.charCodeAt(0); // The charCodeAt() function returns the order of a given character
  }

  // Function to insert a key
  public insert(key: string) {
    // None keys are not allowed
    if (!key) {
      return;
    }
    key = key.toLowerCase(); // Keys are stored in lowercase
    let currentNode = this.root;
    let index = 0; // To store the character index
    for (let level = 0; level < key.length; level++) {
      index = this.getIndex(key[level]);
      if (currentNode.children[index] === null) {
        currentNode.children[index] = new Node(key[level]);
        console.log(String(key[level]) + ' inserted');
      }
      currentNode = currentNode.children[index];
    }
    // Mark the end character as leaf node
    currentNode.markAsLeaf();
    console.log("'" + key + "' inserted");
  }

  // Function to search for a given key in the Trie
  public search(key: string): boolean {
    if (!key) {
      return false; // null key
    }
    key = key.toLowerCase();
    let currentNode: Node = this.root;
    let index = 0;
    for (let level = 0; level < key.length; level++) {
      index = this.getIndex(key[level]);
      if (currentNode.children[index] === null) {
        return false;
      }
      currentNode = currentNode.children[index];
    }
    if (currentNode !== null && currentNode.isEndWord) {
      return true;
    }
    return false;
  }

  public delete(key: string): void {
    if (this.root === null || key === null) {
      console.log('None key or empty trie error');
      return;
    }
    this.deleteHelper(key, this.root, key.length, 0);
  }

  public totalWords(rootN: Node): number {
    let result = 0;
    if (rootN.isEndWord) {
      result += 1;
    }
    for (let i = 0; i < 26; i++) {
      if (rootN.children[i] != null) {
        result += this.totalWords(rootN.children[i]);
      }
    }
    return result;
  }
  public findWords(root: Node): string[] {
    const result = [];
    const chararr = [];
    for (let i = 0; i < 20; i++) {
      chararr.push(null);
    }
    this.getWords(root, result, 0, chararr);
    return result;
  }

  public isFormationPossible(word: string): boolean {
    const posPrefix = this.findPos(this.root, word);
    if (posPrefix === -1) {
      return false;
    }
    const remainingWord = word.slice(posPrefix, word.length);
    const restWordPos = this.findPos(this.root, remainingWord);
    if (posPrefix + restWordPos === word.length) {
      return true;
    }
    return false;
  }

  private findPos(root: Node, word: string): number {
    let level = 0;
    let currentNode = root;
    for (level = 0; level < word.length; level++) {
      const index = word[level].charCodeAt(0) - 'a'.charCodeAt(0);
      if (!currentNode.children[index]) {
        return level;
      }
      currentNode = currentNode.children[index];
    }
    if (currentNode != null && currentNode.isEndWord) {
      return level;
    }
    return -1;
  }
  private getWords(
    root: Node,
    result: string[],
    level: number,
    charArr: string[]
  ) {
    if (root.isEndWord) {
      let temp = '';
      for (let x = 0; x < level; x++) {
        temp += String(charArr[x]);
      }
      result.push(temp);
    }
    for (let i = 0; i < 26; i++) {
      if (root.children[i] != null) {
        charArr[level] = String.fromCharCode(i + 'a'.charCodeAt(0));
        this.getWords(root.children[i], result, level + 1, charArr);
      }
    }
  }

  // Helper Function to return true if currentNode does not have any children
  private hasNoChildren(currentNode: Node): boolean {
    for (let i = 0; i < currentNode.children.length; i++) {
      if (currentNode.children[i] != null) {
        return false;
      }
    }
    return true;
  }

  // Recursive function to delete given key
  private deleteHelper(
    key: string,
    currentNode: Node,
    length: number,
    level: number
  ): boolean {
    let deletedSelf = false;
    if (!currentNode) {
      console.log('Key does not exist');
      return deletedSelf;
    }
    if (level === length) {
      if (this.hasNoChildren(currentNode)) {
        currentNode = null;
        deletedSelf = true;
      } else {
        currentNode.unMarkAsLeaf();
        deletedSelf = false;
      }
    } else {
      const childNode = currentNode.children[this.getIndex(key[level])];
      const childDeleted = this.deleteHelper(key, childNode, length, level + 1);
      if (childDeleted) {
        currentNode.children[this.getIndex(key[level])] = null;
        if (currentNode.isEndWord) {
          deletedSelf = false;
        } else if (!this.hasNoChildren(currentNode)) {
          deletedSelf = false;
        } else {
          currentNode = null;
          deletedSelf = true;
        }
      } else {
        deletedSelf = false;
      }
    }
    return deletedSelf;
  }
}
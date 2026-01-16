class BinarySeachTree {
	constructor() {
		this.root = null
	}
	insert(value) {
		var newNode = new Node(value);
		if(this.root == null) {
			this.root = newNode;
			return this;
		}
		let current = this.root;
		while(current) {
			if(current.val > newNode.val) {
				if(current.left == null) {
					current.left = newNode;
					return this;
				}
				current = current.left;
			} else {
				if(current.right == null) {
					current.right = newNode;
					return this;
				}
				current = current.right;
			}
		}
	}
	can_create(value) {
		if(this.root == null) return false;
		let current = this.root;
		let can_create = false;
		while(current) {
			if(value === '') {
				can_create = true;
				return this;
			} else {
				if(value.includes(current.val)) {
					value = value.replace(current.val,'');
				}
				
				if(value > current.val) {
					current = current.right;
				} else {
					current = current.left
				}
			}
		}
		if(value === '') return true;
		return can_create;
	}
}

class Node {
	constructor(val){
		this.val = val;
		this.left = null;
		this.right = null;
	}
}

const bt = new BinarySeachTree();
const list_of_strings = ["back", "end", "front", "tree"];

for(let string of list_of_strings) {
	bt.insert(string);
}

// console.log(JSON.stringify(bt, null, 2));

console.log(bt.can_create("backend"));
console.log(bt.can_create("frontend"));
console.log(bt.can_create("frontyard"));

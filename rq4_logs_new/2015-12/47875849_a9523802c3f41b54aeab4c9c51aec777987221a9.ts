module tslib.util {
	var hashCode = function(key) {
		if(typeof key === "string") {
			if(key.length > 0) {
				var h = 0;
				for(var i = 0; i < key.length; i++) {
					h = 31 * h + parseInt(key.charAt(i),10);
				}
				return h;
			}
		}else{
			if(key.hashCode) {
				return key.hashCode;
			}else{
				var str = JSON.stringify(key);
				key.hashCode = hashCode(str);
				return key.hashCode;
			}
		}

	}
	class Node<K,V>  {
		public hash:number;
		public key:K;
		public value:V;
		public next:Node<K,V>;
		construct(hash:number, key:K, value:V, next:Node<K,V>) {
			this.hash = hash;
			this.key = key;
			this.value = value;
			this.next = next;
		}

		hashCode():number {
			return hashCode(this.key) ^ hashCode(this.value);
		}
	}
	export class HashMap<K, V>{
		

		

	}
}
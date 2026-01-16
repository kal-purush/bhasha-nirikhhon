class Program {
	input = [1, 2, 3, 4, 5, 6, 7, 8, 9];
	
	forFunc() {
		var out = 0;
		for (var i = 0; i < this.input.length; i++) {
			out += this.input[i];
		}
		console.log(out);
	}
	
	whileFunc() {
		var out = 0;
		var i = 0;
		while (i < this.input.length) {
			out += this.input[i];
			i++;
		}
		console.log(out);
	}
	
	private recursionWorker(index?: number, out?: number): number {
		if (!index) { index = 0; }
		if (!out) { out = 0; }
		if (index >= this.input.length) {
			return out;
		} else {
			out += this.input[index];
		}
	
		return this.recursionWorker(index + 1, out);
	}
	
	recursionFunc() {
		var out = this.recursionWorker();
		console.log(out);
	}
}

var p = new Program();
p.forFunc();
p.whileFunc();
p.recursionFunc();
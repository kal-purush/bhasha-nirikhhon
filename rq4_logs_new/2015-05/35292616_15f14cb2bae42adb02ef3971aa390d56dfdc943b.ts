class Program {
	static end = 10000;

	static printFibonacci(prev1?: number, prev2?: number) {
		// When we have no previous values, assume 0
		if (!prev1) { prev1 = 0; prev2 = 0; }
		
		// The result is the previous two numbers added up
		var result = prev1 + prev2;
		console.log(result);

		// Exit when our result has become larger than 10.000
		if (result >= this.end) { return; }
		
		// Store the previous two results
		prev2 = prev1;
		prev1 = result;

		// We are cheating here, to get over the 0 state.		
		if (result == 0) {
			prev1 = 1;
		}
	
		// Call ourselves again for the next result in the sequence	
		this.printFibonacci(prev1, prev2);
	}
}

Program.printFibonacci();
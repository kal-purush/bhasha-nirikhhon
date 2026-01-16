
// template for tests

module Test {

	export class Test {
		public static tests: Test[] = [];

		constructor(public testname: string, public testfunction: boolean, public success_msg: string) {
			Test.tests.push(this);
		}

		eval() {
			return this.testfunction == true;
		}
	}

	export function printTests() {
		for (var test of Test.tests) {
			document.write("<h4>" + test.testname + "</h4>");
			if (test.eval()) {
				document.write(test.success_msg);
			}
			else { document.write("<p>Test Failed</p>");}
		}
	}

}
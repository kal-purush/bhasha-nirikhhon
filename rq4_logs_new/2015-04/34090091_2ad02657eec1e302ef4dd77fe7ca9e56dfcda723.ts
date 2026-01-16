/// TypeScript Implemetation of FizzBuzz.
/// Write a program that prints the numbers from 1 to 100. 
/// But for multiples of three print “Fizz” instead of the number and for the multiples of five print “Buzz”. 
/// For numbers which are multiples of both three and five print “FizzBuzz”.

interface IFizzbuzz {
    populateList(count: number): void;
    solve(): void;
    print(output: string): void;
}

class Fizzbuzz {
    count: number;
    list: number[];

    constructor(count: number) {
        this.count = count;
        this.populateList(this.count);
    }

    // Populate the list
    populateList(count: number) {
        this.list = new Array<number>();

        for (var x = 0; x <= count; x++) {
            this.list.push(x);
        }
    }

    // Solve the problem; logic lives here
    solve() {
        this.list.forEach(num => {
            if (num % 3 === 0 && num % 5 === 0) {
                this.print("FizzBuzz");
            }
            if (num % 3 === 0) {
                this.print("Fizz");
            }
            if (num % 5 === 0) {
                this.print("Buzz");
            } else {
                this.print(num.toString());
            }
        });
    }

    // Handle the output
    print(output: string) {
        console.log(output);
    }
}

// Produce output
window.onload = () => {
    var count = 1000;
    var fizzBuzz = new Fizzbuzz(count);
    fizzBuzz.solve();
};
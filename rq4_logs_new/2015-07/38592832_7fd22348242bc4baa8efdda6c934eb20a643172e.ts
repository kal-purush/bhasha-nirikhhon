var evens = [0, 2, 4, 6, 8];
var fives: number[];

// Expression bodies
var odds = evens.map(v => v + 1);
var nums = evens.map((v, i) => v + i);

// Statement bodies
nums.forEach(v => {
    if (v % 5 === 0)
        fives.push(v);
});

// Lexical this
var bob: {
    _name: string,
    _friends: string[],
    printFriends: () => void
} = {
        _name: "Bob",
        _friends: [],
        printFriends() {
            this._friends.forEach((f: string) =>
                console.log(this._name + " knows " + f));
        }
    }
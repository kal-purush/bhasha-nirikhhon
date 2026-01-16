function capitalizeEvenLetter(word) {
    let result = '';
    for (let i = 0; i < word.length; i++) {
        if (i % 2 === 1) {
            result += word[i].toUpperCase();
        } else {
            result += word[i];
        }
    }
    return result;
}

console.log(capitalizeEvenLetter("javascript"))

function filterPositiveNumbers(numbers) {
    return numbers.filter((value) => value > 0);
}

console.log(filterPositiveNumbers([-2, 5, 0, 10, -3]));

function doubleNumbers(numbers) {
    return numbers.map((value) => value * 2);
}

console.log(doubleNumbers([1, 4, 8, 8]));


const person = {
    name: "Денис",
    age: 32,
    introduce() {
        console.log(`Привет, меня зовут ${this.name} и мне ${this.age} лет.`)
    }
}

person.introduce();

function isPalindrome(word) {
    const lowerWord = word.toLowerCase();
    const reversedWord = lowerWord.split('').reverse().join('');
    return lowerWord === reversedWord;
}

console.log(isPalindrome("топот"));
console.log(isPalindrome("Ретро"));
console.log(isPalindrome("Шалаш"));
console.log(isPalindrome("МаДаМ"));
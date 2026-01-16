// Function hub to decode the messages below
function decodeMessage(message) {
    return message.split('').reverse().join('');
}

// Task 1: Jumbled Messages
let message1 = " !yako eb ll'uoy dna ,gniog peeK !sgnittes fo yteirav a ni slliks gnidoc esu osla nac uoY !ti teg ll'uoy ,tsisrep uoy fi tub ,tsrif ta drah mees yam gnidoC";
let message2 = "!ecitcarp htiw retteb teg ll‘uoy ,emit ekaT .tsrif ta drah leef lliw gnitirw edoc tuB";
let message3 = "!elpoep rehto morf tnereﬃd on era uoy ,elbuort evah uoy fI .lanoisseforp a ekil leef ot evah t'nod uoY";
let message4 = ".rettam llits yeht ,smargorp llams etirw ylno nac uoy fI .tnemom tcefrep eht rof tiaw t'noD .yadot trats tsuJ";

// Decoding and displaying the messages
console.log(decodeMessage(message1));
console.log(decodeMessage(message2));
console.log(decodeMessage(message3));
console.log(decodeMessage(message4));

// Task 2: My turn
function reverseMessage(message)  {
    return message.split('').reverse().join('');
}

let myMessage1 =  "Hello, this is future Preston. I hope this message finds you well.";
let myMessage2 =  "You are doing great! Sorry about the message scrambling.";
let myMessage3 =  "I wonder what else you can do with these arrays?";
let myMessage4 =  "Welp, this is my last message for now.";

let reversedMessage1 = reverseMessage(myMessage1);
let reversedMessage2 = reverseMessage(myMessage2);
let reversedMessage3 = reverseMessage(myMessage3);
let reversedMessage4 = reverseMessage(myMessage4);

console.log(reversedMessage1);
console.log(reversedMessage2);
console.log(reversedMessage3);
console.log(reversedMessage4);
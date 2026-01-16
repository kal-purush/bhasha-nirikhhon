const accountId = 14453;               // Use `let` if you plan to reassign
let accountEmail = "kriricha0@gmail.com"; // Use `let` for consistency
var accountPassword = "12345";       // Use `let` for consistency
let accountCity = "Jaipur"; 
let accountState;
// Explicitly declare `accountCity`

// Reassign values

accountEmail = "hc@hc.com";
accountCity = "Gaya";
/*
prefer not use var 
because of issue in block scope & functional scope
*/
console.log(accountId);  // Should output 2
console.table([accountId,accountCity,accountEmail,accountPassword,accountState])

var Splitter = require("./Splitter"),
    ClearText = require("./ClearText"),
    GetCharacterCount = require("./GetCharacterCount"),
    IsPrime = require("./IsPrime"),
    GetRawText = require("./GetRawText"),
    Q = require("Q");


/// Return type will be an array of words with count as it's value
function WordCountBuilder(RawText /* Raw Text Getter */,
    ClearText /* This will clear puncation */,
    Splitter /* This will perform custom Split */,
    UniqueValueDecider
) {
    var defer = Q.defer();

    console.log("1. Getter Started > ", new Date());
    RawText().then(function (rawText) {
        rawText = String(rawText).toLowerCase();
        console.log("2. Getter ended , Clear Text started > ", new Date());
        var goodText = ClearText(rawText);
        console.log("3. Clear Text ended , Splitter started > ", new Date());
        var arr = Splitter(goodText)
            , result = [];

        console.log("4. Splitter ended , Loop started > ", new Date());

        for (var key in arr) {
            var item = arr[key];
            if (item !== " ")
                result[item] = GetCharacterCount(result, item);           
        }

        console.log("5. Loop ended.", new Date());
        defer.resolve(result);
    });

    return defer.promise;
}


function GetWordCount(arr) {

    if (typeof arr === "undefined" || arr === null) {
        throw new Error("arr can not be null");
    }
    if (Object.prototype.toString.call(arr) !== "[object Array]") {
        throw new Error("arr must be an array");
    }

    for (var key in arr) {
        console.log("key=" + arr[key]);
    }
}

function GetWordsWithPrimeNumberCount(arr) {

    if (typeof arr === "undefined" || arr === null) {
        throw new Error("arr can not be null");
    }
    if (Object.prototype.toString.call(arr) !== "[object Array]") {
        throw new Error("arr must be an array");
    }
    
    for (var key in arr) {    
        var item = arr[key];
        if (IsPrime(item)) {
            console.log(key + ":=" + item);
        }
    }
    
}



module.exports.WordCountBuilder = WordCountBuilder;
module.exports.GetWordCount = GetWordCount;
module.exports.GetWordsWithPrimeNumberCount = GetWordsWithPrimeNumberCount;
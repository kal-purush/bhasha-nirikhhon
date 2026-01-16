//write a program that can encrypt texts with an alphabetical caesar cipher.
//This cipher can ignore numbers, symbols, and whitespace.

class Constants {
    public static get CIPHER_KEY(): number { return 1; }
    public static get STRING_NO_VALUE(): string { return ""; }
}

interface Failable {
    hasOperationFailed: boolean;
}

interface Encoder extends Failable {
    encode(msg: string): string;
}

class CeaserCipher implements Encoder {
    private validChars: string = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private cipherLen: number = this.validChars.length;
    private cipherKey: number;

    public hasOperationFailed: boolean;

    constructor(cipherKey: number) {
        this.cipherKey = cipherKey;
        this.hasOperationFailed = false;
    }

    private cleanUpInput(msg: string): string {
        msg = msg.replace(" ", "");
        msg = msg.toUpperCase();
        return msg;
    }

    private isValid(msg: string): boolean {
        if (msg.trim() == "")
            return false;

        var isAllLetters: boolean = /[A-Z]/.test(msg);

        if (!isAllLetters)
            return false;

        return false;
    }

    private encodeChar(char: string): string {
        return "";
    }

    public encode(msg: string): string {
        msg = this.cleanUpInput(msg);

        if (!this.isValid(msg)) {
            this.hasOperationFailed = true;
            return Constants.STRING_NO_VALUE;
        }

        var result: string = "";

        for (var i: number; i < msg.length; i++) {
            result += this.encodeChar(msg[i]);
        }

        return msg;
    }
}

function PromptToEncodeAndDisplayResults(): void {
    var msg = prompt("Message to encode");

    var myEncoder: Encoder, result: string;
    myEncoder = new CeaserCipher(Constants.CIPHER_KEY);
    result = myEncoder.encode(msg);

    if (myEncoder.hasOperationFailed)
        alert("Invalid input!")
    else
        alert(result);
}

function IsRepeat(): boolean {
    return confirm("Repeat?");
}

window.onload = () => {
    do {
        PromptToEncodeAndDisplayResults();
    }
    while (IsRepeat());
};
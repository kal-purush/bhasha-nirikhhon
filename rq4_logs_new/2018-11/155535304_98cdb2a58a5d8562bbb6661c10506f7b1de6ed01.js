class Count{
    constructor(str1, str2) {
        this.str1 = str1;
        this.str2 = str2;
    }

    calc() {
        console.log(this);
        this.str1 = this.str1.split('');
        let counter = 0;
        for (let i = 0; i < this.str1.length; i++){
            if (this.str2 === this.str1[i]) {
                counter++;
            }
        }
        return counter;
    }
}


function strCount(str1, str2) {
    let s = new Count(str1, str2);
    return s.calc();
}

console.log(strCount('hello', 'l'));
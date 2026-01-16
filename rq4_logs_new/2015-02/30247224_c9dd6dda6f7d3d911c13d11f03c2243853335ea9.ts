class Person {
    constructor(public name:string, public birthday:Date ) {
    }
}

var p: Person[] = [
    new Person("our baby", new Date())
];

for (var i = 0; i < p.length; i++) {
    var day = p[i].birthday;
    if (day.getMonth() == 2 && day.getDay() == 29) {
        // nameがnaneになっている
        // console.log(p[i].nane + "の誕生日は4年に一度の2月29日です。");
        console.log(p[i].name + "の誕生日は4年に一度の2月29日です。");
    }
    else {
        console.log(p[i].name + "の誕生日は" + day.getMonth() + "月" + day.getDay()
              + "日です。");
    }
}
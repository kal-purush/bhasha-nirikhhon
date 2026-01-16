"use strict";
// normal class
class demo {
    myfun(num, num2) {
        this.name = num;
        this.contact = num2;
        console.log("Name:", num, " ", "Contact", num2);
    }
}
//inheritance
class demo1 extends demo {
}
let obj1 = new demo1;
obj1.myfun("Ram", 96868);
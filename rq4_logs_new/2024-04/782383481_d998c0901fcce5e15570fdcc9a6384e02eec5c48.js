function sayHello() {
    console.log("안녕하세요");
};
// 1회성
setTimeout(sayHello, 1000);

// 주기성
setInterval(setTime, 1000);

console.log(new Date());
console.log(new Date().getDate());
console.log(new Date().getDay());
console.log(new Date().getSeconds());
console.log(new Date().getMinutes());
console.log(new Date().getHours());
console.log(new Date().getFullYear());

function setTime() {
    const time = new Date();
    const 시 = time.getHours().toString();
    const 분 = time.getMinutes().toString();
    const 초 = time.getSeconds().toString();
    const timeH1 = document.querySelector(".time");
    timeH1.innerText = `${시.padStart(2, '0')} : ${분.padStart(2, '0')} : ${초.padStart(2, '0')}`;
}

const 시작_시간 = new Date();

function 경과_시간_계산_함수() {
    const 현재_시간 = new Date();
    const 흐른_시간 = new Date(현재_시간 - 시작_시간);
    const 시 = 흐른_시간.getHours().toString();
    const 분 = 흐른_시간.getMinutes().toString();
    const 초 = 흐른_시간.getSeconds().toString();
    const afterTimeElement = document.querySelector(".after_time");
    afterTimeElement.innerText = `${시.padStart(2, '0')} : ${분.padStart(2, '0')} : ${초.padStart(2, '0')}`;

}

// 주기성
setInterval(경과_시간_계산_함수, 1000);
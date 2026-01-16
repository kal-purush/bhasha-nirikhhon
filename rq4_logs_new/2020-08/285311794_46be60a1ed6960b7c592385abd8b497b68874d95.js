var userId = document.querySelector("#user-id");
var pw1 = document.querySelector("#user-pw1");
var pw2 = document.querySelector("#user-pw2");

userId.onchange = checkid;
pw1.onchange = checkpw;
pw2.onchange = comparepw;

function checkid() {
    if (userId.value.length < 4 || userId.value.length > 8) {
        alert("4~8자리의 영문과 숫자를 사용하세요.");  // 오류 메시지
        userId.select();    // 재입력을 위해 커서 설정
    }
}

function checkpw() {
    if (pw1.value.length != 8) {
        alert("8자리 설정하세요.");
        pw1.value = " ";
        pw1.focus();
    }
}

function comparepw() {
    if (pw2.value != (pw1.value)) {
        alert("비밀번호를 다시 확인 하세요.");
        pw2.value = " ";
        pw2.focus();
    }
}
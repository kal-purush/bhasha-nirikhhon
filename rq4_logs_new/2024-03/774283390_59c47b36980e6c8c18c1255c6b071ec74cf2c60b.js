import axios from "axios";
let result = [];
let randomNum;
const resultArray = [];
const today = new Date();

const dateString = today.toLocaleDateString("ko-KR", {
  year: "numeric",
  month: "long",
  day: "numeric",
});

// 개인 토큰 입력
const token = "YOURTOKEN";
const baseURL = "https://api.github.com/";

const instance = axios.create({
  baseURL,
  headers: {
    "Cache-Control": "no-cache",
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    Authorization: `token ${token}`,
  },
});

function addNumber() {
  randomNum = Math.floor(Math.random() * 45 + 1);
  if (!result.includes(randomNum)) {
    result.push(randomNum);
  }
}

function lottoResult() {
  while (result.length < 6) {
    addNumber();
  }
  console.log(result, "로또 추첨 결과");
  resultArray.push({ [`${resultArray.length}회차`]: [...result] });

  saveLotto().then(() => {
    console.log("POSTING 완료");
    lottoClear();
  });
}

// 로또초기화
function lottoClear() {
  result = [];
}

// 반복기능
function lottoLoop(loopCount) {
  if (!loopCount || typeof loopCount !== "number") return;
  for (let i = 0; i < loopCount; i++) {
    lottoResult();
  }
}

// 로또저장API
async function saveLotto() {
  const response = await instance.post("/repos/gwangseok2/lotto-save/issues", {
    title: `${dateString} 로또 추첨 ${JSON.stringify(result)}`,
    body: JSON.stringify(result),
    label: "bug",
  });
  console.log(response, "스테이터스");
}

// 로또반복
// lottoLoop(3);

// 로또추첨
lottoResult();
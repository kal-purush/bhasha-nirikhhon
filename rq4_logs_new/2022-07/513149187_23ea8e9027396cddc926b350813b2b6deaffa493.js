//백준 제출 할 때 주석 제거
const readFileSyncAddress = '/dev/stdin';

//VSC 테스트 할 때 주석 제거
// const readFileSyncAddress = 'input.txt';


// 1316 그룹 단어 체커
const fs = require('fs');
const input = require('fs').readFileSync('/dev/stdin').toString().split("\n");
const num = Number(input[0]);
let count = 0;

for(let i=1; i<num+1; i++) {
  const words = input[i];
  const letter = [];
  let GroupWord =true;

  for(let j =0; j<words.length; j++){
    if(letter.indexOf(words[j]) === -1) {   
      letter.push(words[j]);
    } else {
      if (letter.indexOf(words[j]) !== letter.length-1) {
        GroupWord = fse;
        break;
        }
      }
    }

    if(GroupWord) {
      count +=1;
  }
}
console.log(count);




/* 문제를 풀기위해 알아야하는 메소드 
1) indexof
aa.indexOf(searchvalue, position)
indexOf : string에서 특정문자열을 찾고, 검색된 문자열이 '첫번째'로 나타나는 위치 index를 리턴
찾는 문자열이 없으면 -1을 리턴하고 대소문자는 구분함

2) push()
배열 추가 : Array.push(), Array.unshift(), Array.splice()
배열 삭제 : Array.pop(), Array.shift(), Array.splice()

3)*/
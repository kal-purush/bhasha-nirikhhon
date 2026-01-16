//Trim()함수 구현(문자열의 좌,우측의 공백을 제거하여 돌려줌)
String.prototype.trim = function(){
 return this.replace(/(^\s*)|(\s*$)/gi, "");
};
 
//LTrim()함수 구현(문자열의 좌측의 공백을 제거하여 돌려줌)
String.prototype.ltrim = function(){
 return this.replace(/^\s*/, "");
};
 
//RTrim()함수 구현(문자열의 우측의 공백을 제거하여 돌려줌)
String.prototype.rtrim = function(){
 return this.replace(/\s*$/, "");
};

//LPad함수 구현(원본문자열이 최대크기보다 작은 경우 대체 문자열로 왼쪽부터 최대크기만큼 채워서 돌려줌)
String.prototype.lPad = function(totalLen,strReplace){ //getLPad(최대크기,대체문자열)
 
 var strAdd  = "";
 var diffLen = totalLen - this.length; //최대크기에서 원본 문자열의 크기를 뺀 후 저장
 
 for(var i = 0; i < diffLen; ++i)
  strAdd += strReplace;  //대체 문자열을 원본 문자열 앞에 추가하여 저장
 
 return strAdd + this; //대체 문자열로 채운 문자열과 원본 문자열을 반환
};
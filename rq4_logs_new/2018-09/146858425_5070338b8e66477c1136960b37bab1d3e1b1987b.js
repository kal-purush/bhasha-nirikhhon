var todayPhoto = [
    {"url":"http://media.daum.net/photo/2841","img":"http://icon.daumcdn.net/w/c/12/05/82877085750988319.jpeg","title":"&quot;뜨면 끝장&quot; 최강 공격헬기 성능이 설마","id":"20120516082207657"}
    ,{"url":"http://media.daum.net/entertain/photo/gallery/?gid=100320","img":"http://icon.daumcdn.net/w/c/12/05/82876693901189319.jpeg","title":"&#39;오늘만&#39; 필리핀 새댁 5개국어 능통 엄친딸","id":"20120516091011626"}
    ,{"url":"http://media.daum.net/photo/4010","img":"http://icon.daumcdn.net/w/c/12/05/82876307459008319.jpeg","title":"[북한 결혼식 풍경] 신랑·신부 &quot;행복합니다&quot;","id":"20120516092605081"}
    ,{"url":"http://sports.media.daum.net/general/gallery/gagsports/index.html","img":"http://icon.daumcdn.net/w/c/12/05/81730673405131839.jpeg","title":"&#39;내가 더 잘해&#39; 후보GK 경기 난입해 선방","id":"20120516100608409"}
    ,{"url":"http://sports.media.daum.net/general/gallery/0516ufc/index.html","img":"http://icon.daumcdn.net/w/c/12/05/81728759258718839.jpeg","title":"양동이의 하이킥에 타바레스 &#39;주춤&#39;","id":"20120516093313331"}
    ,{"url":"http://media.daum.net/entertain/photo/gallery/?gid=100539","img":"http://icon.daumcdn.net/w/c/12/05/81728404408992839.jpeg","title":"이승철 아내와 딸 사진 공개 &quot;사주에. .&quot;","id":"20120516093918544"}
    ,{"url":"http://media.daum.net/photo/3899","img":"http://icon.daumcdn.net/w/c/12/05/81728227037306839.jpeg","title":"생후 6개월에 프랑스로 입양됐던 아이가..","id":"20120516030614331"}
    ,{"url":"http://sports.media.daum.net/general/gallery/STARKIMYUNA/index.html","img":"http://icon.daumcdn.net/w/c/12/05/81727815537682839.jpeg","title":"&#39;교생&#39; 김연아, 스승의날에도 인기폭발","id":"20120516092003892"}
];

var wrap = document.getElementById('wrap');
var paging = document.getElementById('paging');
var nextbtn = document.querySelector('#nextbtn');
var backbtn = document.querySelector('#backbtn');


var lastpage, lastnum; //마지막 페이지 번호, 마지막 페이지에 나타날 사진의 개수
var numInPage; //한 페이지에 나타날 사진의 개수
var setNumInPage;
var page = 1; //페이지 번호 1로 초기화
var firstidx; //각 페이지에서 첫 번째로 나타날 사진의 인덱스



function setIndex(num){ //인자로 한 페이지에 나타내고자 하는 사진의 개수를 준다.
    setNumInPage = num;
    numInPage = setNumInPage; //numInPage 초기화
    lastpage = parseInt(todayPhoto.length/numInPage)+1; //마지막 페이지 계산
    lastnum = todayPhoto.length%numInPage; //마지막 페이지에 보여질 사진 개수 계산
} //처음 한 번만 실행되는 함수

function setFirstIndex(pagenum){
    firstidx = numInPage*(pagenum-1); //첫 사진 인덱스
    if(pagenum===lastpage) //마지막 페이지면
        numInPage=lastnum; //갯수 다르게
    else
        numInPage=setNumInPage; //마지막 페이지 아니면 갯수 원래대로
    return firstidx;
}

function spread(n){
    setFirstIndex(n);

    var pagestr = n+' / '+lastpage;
    var str = '';
    
    for(var i=firstidx; i<firstidx+numInPage; i++){
        str += '<img src="'+todayPhoto[i].img+'"> ';
        str += todayPhoto[i].title;
    }
    wrap.innerHTML = str;
    paging.innerHTML = pagestr;
}


function nextbtnclick(){ // (>)버튼 클릭했을 때
    if(page===lastpage){
        alert("마지막 페이지입니다.");
        return;
    }
    else{
        page+=1;
        spread(page);
    }
}

function backbtnclick(){ // (<)버튼 클릭했을 때
    if(page===1){
        alert("첫 번째 페이지입니다.");
        return;
    }
    else{
        page-=1;
        spread(page);
    }
}


//초기화
setIndex(3);
spread(page);


//이벤트리스너
nextbtn.addEventListener('click', nextbtnclick);
backbtn.addEventListener('click', backbtnclick);
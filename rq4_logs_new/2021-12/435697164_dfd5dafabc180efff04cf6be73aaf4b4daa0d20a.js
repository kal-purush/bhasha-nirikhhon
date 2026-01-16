const score = document.querySelector('.score');
const stars = document.querySelector('.stars');

const MAX = 5; 

let state = {
    score: 0
}

let score_list = [];

//원소의 개수가 5개인 빈 배열을 생성한다 
let stararray = Array(MAX).fill();
//각각의 원소에 접근해서 element요소를 만들어 star div에 하나씩 추가한다
stararray.forEach(() => {
    const star = document.createElement('div');
    star.className = 'star empty';
    stars.appendChild(star);
});


//별 개수를 계산하는 함수 
function calculateScore(e){
    const {width, left} = e.currentTarget.getBoundingClientRect();
    //왼쪽 끝 부터 마우스로 누른 곳 까지의 길이
    const x = e.clientX - left;
    //별 반개
    const half_score = width / MAX;
    //마우스를 누른 위치까지의 별 개수 return
    return Math.floor(x / half_score + 1);
}
//별을 채우는 함수 
function fillStar(star_index){
    const starList = [...stars.children];
    starList.forEach((star,index) => {
        //user가 누른 별의 인덱스가 starList의 인덱스보다 크면 
        //그만큼의 별을 채워야 한다 
        if(star_index > index){
            //만약 둘의 차의 결과가 1이면 반만 채운다 
            if(star_index - index === 1){
                star.className = 'star half';
            }else{
                star.className = 'star full';
            }
        }else{
            star.className = 'star empty';
        }
    });
    //현재의 별 점수 상태를 별 개수의 점수로 저장한다
    state.score = star_index;
    //점수를 표기해주기 위해서 score에 저장한다
    score.innerText = state.score;
}

//state.score로 점수를 얻어와서 score_list에 저장한다
function saveScore() {
    score_list.push(state.score);
    console.log(score_list);
}


//별들을 클릭했을 때에 별들의 색깔이 채워지도록 한다 
stars.addEventListener("click",(e) => {
    fillStar(calculateScore(e));
});

/* //별들 위에서 손가락을 움직이면 별이 채워질 수 있도록 한다 
stars.addEventListener("mousemove", (e) => {
    fillStar(calculateScore(e));
});
*/

//mouse를 떠났을 경우
stars.addEventListener("mouseleave", (e) => {
    fillStar(state.score);
    score.textContent = state.score;
});

//submitbtn를 클릭하면 별 점수가 리스트에 저장이 된다 
submitbtn.addEventListener('click', saveScore);

// Activity
// h1 is score is updating

let show_score1 = document.querySelector("#score1");
let show_score2 = document.querySelector("#score2");
let p1_score=0;
let p2_score=0;
let p1_btn = document.querySelector('#p1-btn');
// console.log(pl_btn);
let p2_btn = document.querySelector('#p2-btn');
// console.log(p2_btn );
let max_score =5;
p1_btn.addEventListener('click',function(){
    
    if(p1_score<max_score &&p2_score<max_score){
        p1_score++;
    show_score1.innerHTML=`${p1_score}`;
    }
    if(p1_score==max_score){show_score1.style.color='green';}
});
p2_btn.addEventListener('click',function(){
    if(p1_score<max_score &&p2_score<max_score){
        p2_score++;
    show_score2.innerHTML=`${p2_score}`;
    }
    if(p2_score==max_score){show_score2.style.color='green';}
});


// h3 is score is updating
// h1 when reach to max score color to green
// button p1 & p2 background color changing
// button reset border is changing  
// input border is changing
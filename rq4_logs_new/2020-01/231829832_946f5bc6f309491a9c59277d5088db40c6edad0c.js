
document.querySelector('form').addEventListener('submit',(event)=>{
  event.preventDefault();
  const total=5;
  let score=0;
 //Get user answers
  const q1=document.forms["questions"].q1.value;
  const q2=document.forms["questions"].q2.value;
  const q3=document.forms["questions"].q3.value;
  const q4=document.forms["questions"].q4.value;
  const q5=document.forms["questions"].q5.value;

  //validation
  for(let i=1;i<=total;i++){
      if(eval('q'+i) === ""){
        alert(`You missed question ${i}`);
        return false;
      }
   }
   const correctAnswers=['b','a','d','b','d'];
   for(let i=1;i<=total;i++){
       if(eval('q'+i) === correctAnswers[i-1]){
         score++;
       }
    }

  alert(`You score is :${score}`);
  //  b a d  d
  document.getElementById('result').innerHTML=`<h3>Your score is <span>${score}</span> out of <span>${total}</span></h3>`

  })
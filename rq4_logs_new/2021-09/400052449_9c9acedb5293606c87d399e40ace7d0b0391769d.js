
   
  const hamburger = document.querySelector(".hamburger");
  const navMenu = document.querySelector(".nav-menu");

  hamburger.addEventListener("click", mobileMenu);

  function mobileMenu() {
    hamburger.classList.toggle("active");
    navMenu.classList.toggle("active");
  }




//  1クイズが出題される
//  2回答する
//  3正誤判定する
//  4全問終わった？
//  Noの場合また1から繰り返す　Yesの場合結果画面へ
// 




// クイズ文、答え、正解を格納する
const quiz = [
  { 
  	question: 'what is the meaning of antiguo？',
    answers: [
       '1, old',
       '2, new',
       '3, simple',
       '4, beatiful'
       ],
    correct: '1, old'
   },
  {　　
  	question: 'what is the meaning of guay？',
  	answers: [
  	    '1, super',
  	    '2, special',
  	    '3, cool',
  	    '4, hot'
  	    ],
  	correct: '3, cool'
   },{
   	  question: 'what is the meaning of inutill？',
   	  answers: [
   	     '1, useless',
   	     '2, effiecient',
   	     '3, fantasitic',
   	     '4, precious'],
   	  correct: '1, useless'
   }
   ];

   const quizLength = quiz.length
   let quizIndex = 0;
   let score = 0;
  


   //長いので何度も使うものは定数を定義する
　　const $button = document.getElementsByClassName('choices') 
    const buttonLength = $button.length;

   // 問題文を出力する 問題の選択肢を表示
   const setupQuiz = () => {
   	document.getElementById('js-question').textContent = quiz[quizIndex].question;
   	let buttonIndex = 0;
   while(buttonIndex < buttonLength) {
   	$button[buttonIndex].textContent = quiz[quizIndex].answers[buttonIndex];
   	buttonIndex++;
   }

   }
   setupQuiz();

   const clickHandler = (e) => {
   	  if(quiz[quizIndex].correct === e.target.textContent) {
   		window.alert('正解！');
   		score++;
   	} else {
   		window.alert('不正解！');
   	}

   	quizIndex++;

   	if(quizIndex < quizLength) {
        setupQuiz();
   	} else{
   		window.alert('終了!あなたの正解数は' + score + '/' + quizLength + 'です！');
   	}
   };
   

   // クリックされたら正解判定
   let handlerIndex = 0;
   while (handlerIndex < buttonLength) {
   	$button[handlerIndex].addEventListener('click',(e) => {
   		clickHandler(e);
   	});
   	handlerIndex++;

   }
   


   
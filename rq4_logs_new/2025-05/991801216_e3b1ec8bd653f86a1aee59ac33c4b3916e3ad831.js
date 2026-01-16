// main.js

// アプリケーションの初期化
window.onload = () => {
  // 最初の画面としてメニューを表示
  showScreen('menu');

  // 問題数選択のドロップダウンを生成
  const questionCountSelect = document.getElementById('question-count-select');
  questionCountSelect.innerHTML = ''; 
  for (let i = 1; i <= 60; i++) {
    const option = document.createElement('option');
    option.value = i;
    option.textContent = `${i}問`;
    questionCountSelect.appendChild(option);
  }
};

// 各画面からのメニューに戻る関数
function backToMenuFromQuiz() {
  if (confirm('クイズを中断してメニューに戻りますか？')) {
    clearInterval(timerId); // quiz.jsのtimerIdを停止
    showScreen('menu'); // utils.jsのshowScreenを使用
  }
}

function backToMenuFromResult() {
  showScreen('menu'); // utils.jsのshowScreenを使用
}

function backToMenuFromPastResults() {
  showScreen('menu'); // utils.jsのshowScreenを使用
}

function backToMenuFromQuestionList() {
  // questionList.jsのcurrentBreadcrumbをクリア
  if (typeof currentBreadcrumb !== 'undefined') {
    currentBreadcrumb = [];
    updateBreadcrumb(); // questionList.jsのupdateBreadcrumbを呼び出し
  }
  showScreen('menu'); // utils.jsのshowScreenを使用
}
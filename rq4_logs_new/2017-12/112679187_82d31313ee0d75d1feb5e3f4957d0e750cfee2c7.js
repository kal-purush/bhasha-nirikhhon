(function(){
	'use strict';

	var start = document.getElementById('start');
	var stop = document.getElementById('stop');
	var result = document.getElementById('result');
	var startTime;
	var isStarted = false;

// スタートボタンが押された時のm処理
	start.addEventListener('click', function() {
		// ボタンがすでに押されてたら何もしない
		if (isStarted === true) {
			return;
		}
		isStarted = true;
		startTime = Date.now(); //経過ミリ秒を取得
		this.className = 'pushed'; //idがstartになっているところにclass名をつける
		stop.className = ''; //stopのクラス名を空にする
		result.textContent = '0.000'; // 中身を0の初期値に戻す
		result.className = 'standby'; // resultのidがついているものにclass名をつけてcssで処理する
	});

// ストップボタンが押された時の処理
	stop.addEventListener('click', function() {
		var elapsedTime;
		var diff;
		if (isStarted === false) {
			return;
		}
		isStarted = false;
		elapsedTime = (Date.now() - startTime) / 1000; //スタートが押された時からstopが押された時の時間を計測
		result.textContent = elapsedTime.toFixed(3);
		this.className = 'pushed';
		start.className = '';
		result.className = '';

//秒数の差の計算
		diff = elapsedTime - 5.0;
		if (Math.abs(diff) < 1.0){ //前後1秒の誤差を許す
			result.className = "perfect";
		}
	});
})();
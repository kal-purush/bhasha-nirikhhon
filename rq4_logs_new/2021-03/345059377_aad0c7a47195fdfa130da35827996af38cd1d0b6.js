$(function () {
  //計算開始ボタンの処理
  $("#btn1").on("click", function () {
    $("#btn1").prop("disabled", true); //連打防止
    $("main").append("3.14159265359"); //円周率序盤は固定値

    let tspan = 10;
    let tstop = 10000;
    var myVar = setInterval(fakepy, tspan);

    //円周率続き（本当は乱数を一定時間ごとに表示させているだけ）
    function fakepy() {
      var num = Math.floor(Math.random() * 9) + 1; //乱数の設定
      $("main").append(num);
    }

    //一定時間後に自動的に処理終了させる
    function myStopFunction() {
      //終了後の処理
      clearInterval(myVar);

      var music = new Audio(
        "https://github.com/Kyosuke-Fukui/camp_html/music/システムエラー・不正解音.mp3"
      );
      music.play();

      $("h1").html("円周率はΠで表しましょう");
    }

    setTimeout(myStopFunction, tstop);
  });

  //リセットボタンの処理
  $("#btn2").on("click", function () {
    window.location.reload();
  });
});
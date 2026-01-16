var test = "テスト";
console.log('test');

//×
console.log(10 * 3);
//割る
console.log(10 / 3);
//余り
console.log(10 % 3);

//JavaScriptのタイプについて
//文字列
var text = "文字列"; //シングル or ダブルで囲う(主にシングル)
console.log('text');

//数値
var num = 1;
console.log('num');

//論理値
var bool = true;
var bool2 = false;
console.log(bool, bool2);

//文字列を数値に変換
console.log(parseInt('100px'));


console.log(Boolean('テスト'));
console.log(Boolean(1));
console.log(Boolean(''));
console.log(Boolean(0));
console.log(Boolean('-1'));
//数値として存在しない場合(空、0)にfalse


$('.box').parallaxFit({
    start: 0,
    end: 500,
    toStyle: {
        left: '300px'
    }
})
// 'use strict';でキーワードで厳密なエラーチェックをするように指示してあげて、ブロックで囲む
'use strict';

{
  // まずはoverlayを表示したいので、関係のある要素を取得していく
  // openの要素を取得したいので、定数名はopenとしてあげて、idを振ったので、getElementById()で取得
  const open = document.getElementById('open');
  // overlayの要素を取得したいので今回はidを振らずにクラスだけ振ってあるので、querySelector()で取得
  const overlay = document.querySelector('.overlay');// querySelector()にはセレクターを指定するので.overlayを指定
  // closeの要素を取得したいので、定数名はcloseとしてあげて、idを振ったので、getElementById()で取得
  const close = document.getElementById('close');
// closeの要素を取得したいので、定数名はmenuとしてあげて、idを振ったので、getElementById()で取得
  const menu = document.getElementById('menu');

  // openをクリックしたら、という処理を書いていきたいので、EventListener()を追加
  open.addEventListener('click', () => {
    overlay.classList.add('show');// showというクラスをつける
    open.classList.add('hide');// バツ印の裏に3本線のアイコンが表示されたままなので、これを消す
  });

  // closeをクリックしたら、という処理を書いていきたいので、EventListener()を追加し上と逆のコードを書く
  close.addEventListener('click', () => {
    overlay.classList.remove('show');
    open.classList.remove('hide');
  });

  // menuをクリックしたら、閉じる処理を書いていきたいので、EventListener()を追加し上と逆のコードを書く
  menu.addEventListener('click', () => {
    overlay.classList.remove('show');
    open.classList.remove('hide');
  });
}
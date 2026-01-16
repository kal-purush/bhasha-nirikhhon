$(function ()
{
  // .hideは要素のうち表示状態のものをアニメーションしながら非表示にする
  $('article').hide().fadeIn(1500);
});



// $(function ()
// {
//   $(document).scroll(function ()
//   {
//     console.log(this)
//     $('article').each(function ()
//     {
//       var elemPos = $(this).offset().top,

//         scroll = $(window).scrollTop(),
//         windowHeight = $(window).height();
//       if (scroll > elemPos - windowHeight + 100)
//       {

//         $(this).addClass('scrollin');
//       }
//     });
//   });


// });
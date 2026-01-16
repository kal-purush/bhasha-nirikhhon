// moving the field labels up on focus
[].slice.call( document.querySelectorAll( '.input' ) ).forEach( function( inputEl ) {
    if( inputEl.value.trim() !== '' ) {
        classie.add( inputEl.parentNode, 'input--filled' );
    }
    inputEl.addEventListener( 'focus', onInputFocus );
    inputEl.addEventListener( 'blur', onInputBlur );
} );

function onInputFocus( ev ) {
      classie.add( ev.target.parentNode, 'input--filled' );
}

function onInputBlur( ev ) {
      if( ev.target.value.trim() === '' ) {
            classie.remove( ev.target.parentNode, 'input--filled' );
      }
}

// copy question text to clipboard
function copyToClip(str) {
	var textarea = $('.textarea');
  function listener(e) {
    e.clipboardData.setData('text/plain', str);
    e.preventDefault();
  }
  document.addEventListener('copy', listener);
  document.execCommand('copy');
  document.removeEventListener('copy', listener);
};

$('.fa-clipboard').on('click', function(){
	$(this).addClass('the-pulse');
	$(this).one('webkitAnimationEnd oanimationend msAnimationEnd animationend',   
    function(e) {
    // code to execute after animation ends
      $(this).removeClass('the-pulse');
    });
})
// Opening: Digital Watch

const showHour = () =>{

  const clock = document.querySelector('time');

  let oclock = new Date();
  let hh = oclock.getHours();
  let mm = oclock.getMinutes();
  let ss = oclock.getSeconds();
  
  hh = hh < 10 ? '0'+ hh : hh; 
  mm = mm < 10 ? '0'+ mm : mm; 
  ss = ss < 10 ? '0'+ ss : ss; 

  clock.innerText = hh +':'+ mm +':'+ ss;
}

showHour()
setInterval(showHour, 1000);

// Closing: Digital Watch

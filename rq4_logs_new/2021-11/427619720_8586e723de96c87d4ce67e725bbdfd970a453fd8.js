var message=new Array();var reps=2;var speed=250;var p=message.length;var T="";var C=0;var mC=0;var s=0;var sT=null;if(reps<1)reps=1;function doTheThing(){A()}
function A(){s++;if(s>13){s=1}
if(s==1){document.title='E_ '}
if(s==2){document.title='Es_ '}
if(s==3){document.title='Esh_ '}
if(s==4){document.title='Eshx_ '}
if(s==5){document.title='Eshxn_ '}
if(s==6){document.title='Eshxn_ '}
if(s==7){document.title='Eshx_ '}
if(s==8){document.title='Esh_ '}
if(s==9){document.title='Es_ '}
if(s==10){document.title='E_ '}
if(s==11){document.title='_ '}
if(s==12){document.title='_ '}
if(s==13){document.title='_ '}
if(C<(13*reps)){sT=setTimeout("A()",speed);C++}else{C=0;s=0;mC++;if(mC>p-1)mC=0;sT=null;doTheThing()}}
doTheThing();
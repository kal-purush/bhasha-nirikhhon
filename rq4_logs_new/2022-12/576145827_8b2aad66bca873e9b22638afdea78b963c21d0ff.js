<script language="JavaScript">
<!-- hide from old browsers
function pacecalc(form)
{
 if (form.hour.value==null||form.hour.value.length==0){ 
  form.hour.value=0;
 } 

 if (form.minute.value==null||form.minute.value.length==0){ 
  form.minute.value=0;
 }

 var secs = 0 + 3600 * form.hour.value + 60 * form.minute.value;

 if (secs != 0 && form.dist.value.length != 0){ 
  var secperkilo = secs/form.dist.options[form.dist.selectedIndex].value;
  var time5k = Math.ceil(secperkilo * 5);
  var min = time5k % 3600;
  var hour = (time5k - min) / 3600;
  var sec = Math.ceil(min % 60);
  min = (min - sec) / 60
  form.time5k.value= "" +hour + ":" + min + ":" + sec;

  var time1k = Math.ceil(secperkilo * 10);
  min = time1k % 3600;
  hour = (time1k - min) / 3600;
  sec = Math.ceil(min % 60);
  min = (min - sec) / 60
  form.time1k.value= "" + hour + ":" + min + ":" + sec;
 }
 return;
}

function speed(form)
{

 if (form.min.value==null||form.min.value.length==0){ 
  form.min.value="0";
 } 
 if (form.sec.value==null||form.sec.value.length==0){ 
  form.sec.value="0";
 } 

 var d2 = form.d2.options[form.d2.selectedIndex].value;
 var presecs = (form.min.value*60) + form.sec.value/1;

 var postsecs = Math.floor(presecs / 1000 * d2);
 if (postsecs != 0){ 
  var min = postsecs % 3600;
  var hour = (postsecs - min) / 3600;
  var sec = Math.ceil(min % 60);
  min = (min - sec) / 60
  form.totaltime.value = "" + hour + ":" + min + ":" + sec;
 }
 return;
}

function estcalc(form)
{
 if (form.hour.value==null||form.hour.value.length==0){ 
  form.hour.value="0";
 } 
 if (form.min.value==null||form.min.value.length==0){ 
  form.min.value="0";
 } 
 if (form.sec.value==null||form.sec.value.length==0){ 
  form.sec.value="0";
 } 

 var d1 = form.d1.options[form.d1.selectedIndex].value;
 var d2 = form.d2.options[form.d2.selectedIndex].value;
 var presecs = (form.hour.value*3600) + (form.min.value*60) + form.sec.value/1;

 // T2 = T1 x (D2/D1)<sup>1.07</sup>
 var postsecs = Math.floor(presecs * Math.pow(d2 / d1, 1.07));
 if (postsecs != 0){ 
  var min = postsecs % 3600;
  var hour = (postsecs - min) / 3600;
  var sec = Math.ceil(min % 60);
  min = (min - sec) / 60
  form.posttime.value = "" + hour + ":" + min + ":" + sec;
 }
 return;
}

function totalcalc(form)
{
 if (form.min.value==null||form.min.value.length==0){ 
  form.min.value="0";
 } 
 if (form.sec.value==null||form.sec.value.length==0){ 
  form.sec.value="0";
 }
 if (form.hour.value==null||form.hour.value.length==0){ 
  form.hour.value="0";
 } 

 var d2 = form.d2.options[form.d2.selectedIndex].value;
 var d1 = form.d1.options[form.d1.selectedIndex].value;
 var presecs = (form.hour.value*3600) + (form.min.value*60) + form.sec.value;
 var one = Math.round(presecs / (d1 / 100) );
 var postsecs = Math.round(one * (d2 / 1000));
 if (postsecs != 0){ 
  var min = postsecs % 3600;
  var hour = (postsecs - min) / 3600;
  var sec = Math.ceil(min % 60);
  min = (min - sec) / 60
  form.totaltime.value = "" + hour + ":" + min + ":" + sec;
 }
 return;
}
<!-- done hiding from old browsers -->
</script>
 <script language="JavaScript">
<!-- hide this script tag's contents from old browsers
function getmin(time) {
if (time/60<1) mins = 0;
 else {
 mins = parseInt(time/60)};   
 return mins; 
}

function getsec(time) {
if (time/60<1) mins = 0;
 else {
 mins = parseInt(time/60)};   
  secs = parseInt(time-60*mins);
  return secs;    
}

function computeForm(form){
 // if ((form.tmin.value == null ||
//form.tmin.value.length == 0) || (form.tsec.value == null ||
//form.tsec.value.length == 0) ) { return; }

var time5k = (form.tmin.value*60)+(form.tsec.value*1);
if (form.effort[0].checked) timet = (time5k/5)/0.9;
else {
timet = (0.98*time5k/5)};
t400 = parseInt(timet*0.4);
tm400 = getmin(t400);
ts400 =getsec(t400);

if (form.effort[0].checked) timet = (time5k/5)/0.9;
else {
timet =(time5k/5)};
t800 = parseInt(timet*0.8);
tm800 = getmin(t800);
ts800 = getsec(t800);



if (form.effort[0].checked) timet = (time5k/5)/0.9;
else {
timet = (1.02*time5k/5)};

t1200 = parseInt(timet*1.2);
tm1200 = getmin(t1200);
ts1200 = getsec(t1200);

if (form.effort[0].checked) timet = (time5k/5)/0.9;
else {
timet = (1.04*time5k/5)};
t1600 = parseInt(timet*1.6);
tm1600 =getmin(t1600);
ts1600 = getsec(t1600);

form.tm400.value = tm400;
form.ts400.value = ts400;
form.tm800.value = tm800;
form.ts800.value = ts800;
form.tm1200.value = tm1200;
form.ts1200.value = ts1200;
form.tm1600.value = tm1600;
form.ts1600.value = ts1600;
 } 

function clearForm(form) { 
form.tmin.value = ""; form.tsec.value = "";
form.tm400.value = "";form.tm800.value = ""; 
form.tm1200.value = "";form.tm1600.value = "";form.ts400.value = "";form.ts800.value = ""; 
form.ts1200.value = "";form.ts1600.value = "";}

browserName = navigator.appName;
browserVer = parseInt(navigator.appVersion);
if (browserVer >= 3) version = "n3";
else {
version = "n2";}
if (version == "n3") {
  // on images
  navb1a_1 = new Image(107, 33);
  navb1a_1.src="images/homeb.jpg";
  navb1b_1 = new Image(107, 33);
  navb1b_1.src="images/prdb.jpg";
  navb1c_1 = new Image(107, 33);
  navb1c_1.src="images/intb.jpg";
  navb1d_1 = new Image(107, 33);
  navb1d_1.src="images/hrb.jpg";
  navb1e_1 = new Image(107, 33);
  navb1e_1.src="images/calb.jpg";
  navb1f_1 = new Image(107, 33);
  navb1f_1.src="images/linkb.jpg";
  // off images
  navb1a_0 = new Image(107, 33);
  navb1a_0.src="images/homea.jpg";
  navb1b_0 = new Image(107, 33);
  navb1b_0.src="images/prda.jpg";
  navb1c_0 = new Image(107, 33);
  navb1c_0.src="images/inta.jpg";
   navb1d_0 = new Image(107, 33);
  navb1d_0.src="images/hra.jpg";
  navb1e_0 = new Image(107, 33);
  navb1e_0.src="images/cala.jpg";
  navb1f_0 = new Image(107, 33);
  navb1f_0.src="images/linka.jpg";
}  

function imgchg(imgst,imgName) {
   if (version == "n3") {
     imgc = eval(imgName + "_" + imgst + ".src");
     document [imgName].src = imgc;
   }
}
</script>
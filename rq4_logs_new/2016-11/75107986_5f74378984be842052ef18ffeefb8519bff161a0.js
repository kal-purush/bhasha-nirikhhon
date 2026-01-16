var obj = {
  "facts": [
    {
      "intro": "E-cigarettes are about as safe as other smoking cessation<br/>products approved by the FDA. Compared to cigarettes, they are",
      "image": "1",
      "fact": "MUCH LESS TOXIC",
      "factjustifier": "\'Cadmium, lead and nickel have also been detected but in trace levels only, comparable with levels in Nicorette inhalers\'",
      "Citation": "Citation: <a href=\'http://www.ncbi.nlm.nih.gov/pmc/articles/PMC3850892/\'>A fresh look at tobacco harm reduction - US National Library of Medicine</a>"
    },
    {
      "intro": "E-cigarettes are being used by many to kick a dangerous habit.<br/>E-cigarettes can help a user to:",
      "image": "2",
      "fact": "REDUCE TOBACCO USE",
      "factjustifier": "\'Long-term e-Cigarette use can decrease cigarette consumption in smokers not willing to quit\'",
      "Citation": "Citation: <a href=\'http://www.ncbi.nlm.nih.gov/pubmed/23873169\'>Effectiveness and tolerability of electronic cigarettes - US National Library of Medicine</a>"
    },
    {
      "intro": "So what's actually in an e-cigarette?<br/>One of their main ingredients is: ",
      "image": "3",
      "fact": "PROPYLENE GLYCOL",
      "factjustifier": "Propylene Glycol is used in many foods and drinks, and is used as a flavour carrier, as well in this case as a vapour producer. It is also an Asthma Inhalers main ingredient.",
      "Citation": "Citation: <a href=\'http://www.ecita.org.uk/blog/index.php/how-toxic-is-e-liquid/\'>How toxic is e-liquid? - ECITA</a>"
    },
    {
      "intro": "So what's actually in an e-cigarette?<br/>Another of their main ingredients is: ",
      "image": "4",
      "fact": "VEGETABLE GLYCERINE",
      "factjustifier": "Some people have a sensitivity to Propylene Glycol so they may prefer a Vegetable glycerine based e-liquid, although almost all are blends.",
      "Citation": "Citation: <a href=\'http://www.ecita.org.uk/blog/index.php/how-toxic-is-e-liquid/\'>How toxic is e-liquid? - ECITA</a>"
    },
    {
      "intro": "So what's actually in an e-cigarette?<br/>Another of their main ingredients is: ",
      "image": "5",
      "fact": "NICOTINE",
      "factjustifier": "Nicotine is added to allow these to be used as an alternative to smoking. This nicotine is considered relatively safe.",
      "Citation": "Citation: <a href=\'http://www.ecita.org.uk/blog/index.php/how-toxic-is-e-liquid/\'>How toxic is e-liquid? - ECITA</a>"
    },
    {
      "intro": "So what's actually in an e-cigarette?<br/>Another of their main ingredients is: ",
      "image": "6",
      "fact": "FLAVOURINGS",
      "factjustifier": "Many people prefer flavoured e-liquid. As such, Food safe flavourings are commonly added.",
      "Citation": "Citation: <a href=\'http://www.mdpi.com/1660-4601/10/10/5146\'>http://www.mdpi.com/1660-4601/10/10/5146</a>"
    },
    {
      "intro": "One issue we're finding with much of the information is misunderstanding.<br/> For example confusing:",
      "image": "8",
      "fact": "FLUORINE AND FREON",
      "factjustifier": "Fluorine is found in any water containing product, and freon is found in Fridges. Not e-cigarettes.",
      "Citation": "Citation: <a href=\'http://www.ecita.org.uk/blog/index.php/how-toxic-is-e-liquid/\'>How toxic is e-liquid? - ECITA</a>"
    },
    {
      "intro": "Another issue is making ridiculous claims such as e-cigarettes contain Lithium. <br/> Yes, this is true, but you're not Inhaling the ",
      "image": "9",
      "fact": "BATTERIES!",
      "factjustifier": "Almost all electronic devices humans use contain Lithium, so this is meaningless.",
      "Citation": "Citation: <a href=\'http://www.totallywicked-eliquid.com/blog/447/exploding-e-cigs-some-important-information/\'>Exploding e-cigs – some important information.</a>"
    },
    {
      "intro": " We've seen claims such as Ecigarettes contain Ethylbenzene. <br/> This is a true but ignores the: ",
      "image": "10",
      "fact": "CONCLUSIONS.",
      "factjustifier": "On the same page as this claim is made, in the smallest possible writing, you'll find that this is found to be 9 to 405 times less than cigarette smoke.",
      "Citation": "Citation: <a href=\'http://tobaccocontrol.bmj.com/content/early/2013/03/05/tobaccocontrol-2012-050859.abstract\'>Levels of selected carcinogens and toxicants in vapour from electronic cigarettes</a>"
    },
    {
      "intro": "We'll leave the reasons as to why these claims are being made up to you<br/>Please research this topic and form your:",
      "image": "11",
      "fact": "OWN OPINION.",
      "factjustifier": "We aren't a political group, nor are we profiting at all from this project. We hope you find it useful.",
      "Citation": ""
    },
    {
      "intro": "Please feel free to explore the rest of this website. <br/> There is lots of: ",
      "image": "11",
      "fact": "FACTUAL INFORMATION",
      "factjustifier": "to be found here. If you're new to vaping or looking to learn more, take a look around!",
      "Citation": ""
    },
  ]
};
var numberofobjects = 11;
var randomvr = 0;

function getmore(actionvar) {
  if(actionvar=="add") {
    randomvr = randomvr + 1;
    if(randomvr>(numberofobjects-1)) {
      randomvr = 0;
    }
  } else {
    randomvr=actionvar;
  }
	document.getElementById("intro").innerHTML = "<h4>"+obj.facts[randomvr].intro+"</h4>";
	document.getElementById("fact").innerHTML = "<h1>"+obj.facts[randomvr].fact+"</h1>";
	document.getElementById("factjustifier").innerHTML = "<h3>"+obj.facts[randomvr].factjustifier+"</h3>";
	document.getElementById("citation").innerHTML = obj.facts[randomvr].Citation;
	document.getElementById("bgimageholder").style.backgroundImage = "url('img/"+obj.facts[randomvr].image+".png')";
	document.getElementById("bgimageholder").style.backgroundImage = "url('img/"+obj.facts[randomvr].image+".png')";
	document.getElementById("mobim").innerHTML = "<img src='img/"+obj.facts[randomvr].image+".png' width='40%' height='40%'>";
}

var currentset = "";
var previousset = "";

function changeColour(cite) {
  if(currentset=="") {   //check to see if currentset is set
    currentset = cite;
  } 
  else {
    previousset = currentset;
    currentset = cite;
    document.getElementById("cite"+previousset).style.backgroundColor = "";  //removes highlighted background from previously selected cite
  }
  document.getElementById("cite"+currentset).style.backgroundColor = "#BFDFFF"; //adds bg to currently selected cite
}
function moveTextToVapingBoxConfirm() {
  var nickToCopy = document.getElementById('nickinitial').value;
  document.getElementById('nickconfirm').value = nickToCopy;
  var emailToCopy = document.getElementById('youremailinitial').value;
  document.getElementById('youremailconfirm').value = emailToCopy;
  var testimonialToCopy = document.getElementById('vapingboxinitial').value;
  document.getElementById('vapingboxconfirm').value = testimonialToCopy;
}
function submitForm() {
  var testimonialToCopy = document.getElementById('vapingboxconfirm').value;
  window.location.href = '/thankyou.php?testimonial=' + testimonialToCopy;
}
$('a').click(function(){
	$('html, body').animate({
	scrollTop: $( $.attr(this, 'href') ).offset().top
	}, 600);
return false;
});
function getQueryVariable(variable)
{
       var query = window.location.search.substring(1);
       var vars = query.split("&");
       for (var i=0;i<vars.length;i++) {
               var pair = vars[i].split("=");
               if(pair[0] == variable){return pair[1];}
       }
       return(false);
}
function goToCorrectFact() {
  var first = getQueryVariable("id");
  if(first!=false){
    getmore(first);
  }
}

function fbShare(url, title, descr, image, winWidth, winHeight) {
    var winTop = (screen.height / 2) - (winHeight / 2);
    var winLeft = (screen.width / 2) - (winWidth / 2);
    window.open('http://www.facebook.com/sharer.php?s=100&p[title]=' + title + '&p[summary]=' + descr + '&p[url]=' + url + '&p[images][0]=' + image, 'sharer', 'top=' + winTop + ',left=' + winLeft + ',toolbar=0,status=0,width=' + winWidth + ',height=' + winHeight);
}
!function(d,s,id){var js,fjs=d.getElementsByTagName(s)[0];if(!d.getElementById(id)){js=d.createElement(s);js.id=id;js.src="https://platform.twitter.com/widgets.js";fjs.parentNode.insertBefore(js,fjs);}}(document,"script","twitter-wjs");

    function verifyform() {
      var pnick = document.testimonialform.nick.value;
      var pemail = document.forms["testimonialform"]["youremail"].value;
      var ptestimonial = document.forms["testimonialform"]["testimonial"].value;
      if (pnick == null || pnick == "" || pemail == null || pemail == "" || ptestimonial == null || ptestimonial == "") {
        alert("Please ensure you've filled out your name, your email, and your message at least.");
        return false;
      } else {
        document.forms["testimonialform"].submit()
      }
    }
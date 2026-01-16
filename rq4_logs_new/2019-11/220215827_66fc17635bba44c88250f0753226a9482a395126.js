/**
* @param {HTMLDivElement} boxKontakt - Control boxKontakt div
*/
const boxKontakt = document.querySelector('.boxKontakt');

/**
*  @param {HTMLDivElement} boxKontaktHidden - Control the hidden content in boxKontakt div
*/
const boxKontaktHidden = document.querySelector('.boxKontakt .hideInMobile');

/**
*  @param {HTMLDivElement} boxKontaktDownButton - Control boxOmMboxKontakt pointing down (Not a <i> Element)
*/  
const boxKontaktDownButton = document.querySelector('.boxKontakt .downToggleButton');

/**
*  @param {HTMLDivElement} boxKontaktUpButton - Control boxKontakt button pointing up (Not a <i> Element)
*/
const boxKontaktUpButton = document.querySelector('.boxKontakt .upToggleButton');

/**
*  @param {string} hideContent - Hides content
*/
const hideContent = 'none';

/**
*  @param {string} showContent - show content
*/
const showContent = 'block';

/**
*  @param {string} showContent - show content
*/
const showContentInline = 'inline-block';


/**
*  @param {string} defaultMobileSize - the default mobile size 1.5rem
*/
const defaultMobileSize = '1.5rem';



//On click this box epands and shows its content - (In mobile)
  boxKontakt.addEventListener("click", function() {
    if (window.innerWidth <= 767) {
      boxKontakt.style.height = "14rem";
      boxKontaktDownButton.style.display = hideContent;
      boxKontaktUpButton.style.display = showContentInline;
      setTimeout(function(){ boxKontaktHidden.style.display = showContent; }, 1);
    }
  });

//If unfolded this box revert to its start size and hide content - (In mobile)
  boxKontakt.addEventListener("click", function() {
    if (window.innerWidth <= 767 &&  boxKontaktHidden.style.display == showContent) {
      boxKontakt.style.height = defaultMobileSize;
      setTimeout(function(){ boxKontaktHidden.style.display = hideContent; }, 1);
      boxKontaktDownButton.style.display = showContentInline;
      boxKontaktUpButton.style.display = hideContent;
    }
  });



/**
* @param {HTMLDivElement} boxOmMig - Control boxOmMig div
*/
const boxOmMig = document.querySelector('.boxOmMig');

/**
*  @param {HTMLDivElement} boxOmMigHidden - Control the hidden content in boxOmMig div
*/
const boxOmMigHidden = document.querySelector('.boxOmMig .hideInMobile');

/**
*  @param {HTMLDivElement} boxOmMigDownButton - Control boxOmMigbutton pointing down (Not a <i> Element)
*/  
const boxOmMigDownButton = document.querySelector('.boxOmMig .downToggleButton');

/**
*  @param {HTMLDivElement} boxOmMigUpButton - Control boxOmMig button pointing up (Not a <i> Element)
*/
const boxOmMigUpButton = document.querySelector('.boxOmMig .upToggleButton');

//On click this box epands and shows its content - (In mobile)
  boxOmMig.addEventListener("click", function() {
    if (window.innerWidth <= 767) {
      boxOmMig.style.height = "34rem";
      boxOmMigDownButton.style.display = hideContent;
      boxOmMigUpButton.style.display = showContentInline;
      setTimeout(function(){ boxOmMigHidden.style.display = showContent; }, 1);
    }
  });
//If unfolded this box revert to its start size and hide content - (In mobile)
  boxOmMig.addEventListener("click", function() {
    if (window.innerWidth <= 767 &&  boxOmMigHidden.style.display == showContent) {
      boxOmMig.style.height = defaultMobileSize;
      setTimeout(function(){ boxOmMigHidden.style.display = hideContent; }, 1);
      boxOmMigDownButton.style.display = showContentInline;
      boxOmMigUpButton.style.display = hideContent;
    }
  });



/**
* @param {HTMLDivElement} boxUtbildning - Control boxUtbildning div
*/
const boxUtbildning = document.querySelector('.boxUtbildning');

/**
*  @param {HTMLDivElement} boxUtbildningHidden - Control the hidden content in boxUtbildning div
*/
const boxUtbildningHidden = document.querySelector('.boxUtbildning .hideInMobile');

/**
*  @param {HTMLDivElement} boxUtbildningDownButton - Control boxUtbildning pointing down (Not a <i> Element)
*/  
const boxUtbildningDownButton = document.querySelector('.boxUtbildning .downToggleButton');

/**
*  @param {HTMLDivElement} boxUtbildningUpButton - Control boxUtbildning button pointing up (Not a <i> Element)
*/
const boxUtbildningUpButton = document.querySelector('.boxUtbildning .upToggleButton');


//On click this box epands and shows its content - (In mobile)
  boxUtbildning.addEventListener("click", function() {
    if (window.innerWidth <= 767) {
      boxUtbildning.style.height = "16rem";
      boxUtbildningDownButton.style.display = hideContent;
      boxUtbildningUpButton.style.display = showContentInline;
      setTimeout(function(){ boxUtbildningHidden.style.display = showContent; }, 1);
    }
  });
//If unfolded this box revert to its start size and hide content - (In mobile)
  boxUtbildning.addEventListener("click", function() {
    if (window.innerWidth <= 767 &&  boxUtbildningHidden.style.display == showContent) {
      boxUtbildning.style.height = defaultMobileSize;
      setTimeout(function(){ boxUtbildningHidden.style.display = hideContent; }, 1);
      boxUtbildningDownButton.style.display = showContentInline;
      boxUtbildningUpButton.style.display = hideContent;
    }
  });
  



/**
* @param {HTMLDivElement} boxJobb - Control boxJobb div
*/
const boxJobb = document.querySelector('.boxJobb');

/**
*  @param {HTMLDivElement} boxJobbHidden - Control the hidden content in boxJobb div
*/
const boxJobbHidden = document.querySelector('.boxJobb .hideInMobile');

/**
*  @param {HTMLDivElement} boxJobbDownButton - Control boxJobb pointing down (Not a <i> Element)
*/  
const boxJobbDownButton = document.querySelector('.boxJobb .downToggleButton');

/**
*  @param {HTMLDivElement} boxJobbUpButton - Control boxJobb button pointing up (Not a <i> Element)
*/
const boxJobbUpButton = document.querySelector('.boxJobb .upToggleButton');


//On click this box epands and shows its content and hide content - (In mobile)
  boxJobb.addEventListener("click", function() {
    if (window.innerWidth <= 767) {
      boxJobb.style.height = "27rem";
      boxJobbDownButton.style.display = hideContent;
      boxJobbUpButton.style.display = showContentInline;
      setTimeout(function(){ boxJobbHidden.style.display = showContent; }, 1);
    }
  });
  //If unfolded this box revert to its start size - (In mobile)
  boxJobb.addEventListener("click", function() {
    if (window.innerWidth <= 767 &&  boxJobbHidden.style.display == showContent) {
      boxJobb.style.height = defaultMobileSize;
      setTimeout(function(){ boxJobbHidden.style.display = hideContent; }, 1);
      boxJobbDownButton.style.display = showContentInline;
      boxJobbUpButton.style.display = hideContent;
    }
  });




/**
 * When the window is resized every box is reseted so that no Javascript size is excedentaly saved during the resize
 * Every size should be the same in CSS 
*/

  window.addEventListener('resize', function() {
  if(window.innerWidth <= 767){
      boxKontakt.style.height = defaultMobileSize;
      boxKontaktHidden.style.display = hideContent;
      boxOmMig.style.height = defaultMobileSize;
      boxOmMigHidden.style.display = hideContent;
      boxUtbildning.style.height = defaultMobileSize;
      boxUtbildningHidden.style.display = hideContent;
      boxJobb.style.height = defaultMobileSize;
      boxJobbHidden.style.display = hideContent;
      boxKontaktDownButton.style.display = showContentInline;
      boxKontaktUpButton.style.display = hideContent;
      boxUtbildningDownButton.style.display = showContentInline;
      boxOmMigUpButton.style.display = hideContent;
      boxOmMigDownButton.style.display = showContentInline;
      boxUtbildningUpButton.style.display = hideContent;
      boxJobbDownButton.style.display = showContentInline;
      boxJobbUpButton.style.display = hideContent;
  }if(window.innerWidth >= 768 && window.innerWidth <= 991) {
      boxKontakt.style.height = "17rem";
      boxKontaktHidden.style.display = showContent;
      boxOmMig.style.height = "16rem";
      boxOmMigHidden.style.display = showContent;
      boxUtbildning.style.height = "14rem";
      boxUtbildningHidden.style.display = showContent;
      boxJobb.style.height = "29rem";
      boxJobbHidden.style.display = showContent;
      boxKontaktDownButton.style.display = hideContent;
      boxKontaktUpButton.style.display = hideContent;
      boxOmMigDownButton.style.display = hideContent;
      boxOmMigUpButton.style.display = hideContent;
      boxUtbildningDownButton.style.display = hideContent;
      boxUtbildningUpButton.style.display = hideContent;
      boxJobbDownButton.style.display = hideContent;
      boxJobbUpButton.style.display = hideContent;
    }else if(window.innerWidth >= 992 && window.innerWidth <= 1199){
      boxKontakt.style.height = "17rem";
      boxKontaktHidden.style.display = showContent;
      boxOmMig.style.height = "15rem";
      boxOmMigHidden.style.display = showContent;
      boxUtbildning.style.height = "17rem";
      boxUtbildningHidden.style.display = showContent;
      boxJobb.style.height = "17rem";
      boxJobbHidden.style.display = showContent;
      boxKontaktDownButton.style.display = hideContent;
      boxKontaktUpButton.style.display = hideContent;
      boxOmMigDownButton.style.display = hideContent;
      boxOmMigUpButton.style.display = hideContent;
      boxUtbildningDownButton.style.display = hideContent;
      boxUtbildningUpButton.style.display = hideContent;
      boxJobbDownButton.style.display = hideContent;
      boxJobbUpButton.style.display = hideContent;
    }else if(window.innerWidth >= 1200){
      boxKontakt.style.height = "15rem";
      boxKontaktHidden.style.display = showContent;
      boxOmMig.style.height = "16rem";
      boxOmMigHidden.style.display = showContent;
      boxUtbildning.style.height = "15rem";
      boxUtbildningHidden.style.display = showContent;
      boxJobb.style.height = "16rem";
      boxJobbHidden.style.display = showContent;
      boxKontaktDownButton.style.display = hideContent;
      boxKontaktUpButton.style.display = hideContent;
      boxOmMigDownButton.style.display = hideContent;
      boxOmMigUpButton.style.display = hideContent;
      boxUtbildningDownButton.style.display = hideContent;
      boxUtbildningUpButton.style.display = hideContent;
      boxJobbDownButton.style.display = hideContent;
      boxJobbUpButton.style.display = hideContent;
    }
  }
  )
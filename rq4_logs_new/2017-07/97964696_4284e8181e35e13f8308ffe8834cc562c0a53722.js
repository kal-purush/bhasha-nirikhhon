/*LOCALIZATION VARAIBLES*/
var countryCodeLocation = "RU"; // "RU", "RO", "TL"

var nameList,
    madeOrderOnSum,
    wasOrdered,
    left,
    shared,
    usersOnline,
    orderedCallback,
    packsLeft,
    codeEmpty,
    codeOk,
    codeWrong,
    packName,
    discountPack,
    leftSingle,
    oneDollarPacktext,
    strarSign,
    allready,
    peopleGotForOneDollar,
    madeOrderOnCount;
    
/* END LOCALIZATION VARAIBLES*/


/* Общие настройки */

var productQuantity = 60; // Желательно > 50.

var intervalTime = 22000; // Задержка папапов.
var mobileFormBreakPoint = 767; //В случае наличия разыных форм для мобилы и десктопа - тут можно указать разрешение, на котором форма будет меняться.



var genderNames = 'both'; // 'men' если нужны только мужские имена. или 'women' если только женские. или 'both' если и те и те.


/*Теги для попапов заказов*/

var tagOnlineStart = '<div><i class="everad-sprite everad-sprite-online_user"></i><span>';
var tagCartStart = '<div><i class="everad-sprite everad-sprite-bucket"></i><span>';
var tagCallBackStart = '<div><i class="everad-sprite everad-sprite-callback"></i><span>';
var tagStartSpan = '<span>';
var tagEndSpan = '</span>';
var tagEndDivAndSpan = '</span></div>';
var tagBlinkSpan = '<span class="blink_me">';
var tagBlinkAnim = '<span class="blink">';

/*Конец тегов*/


/*Включатели функций*/

var modalsClone = true; // клонирование модалок с формой.
var orderPopups = true; // всплывающие попапы с заказами.
var checkCode = true;
var todaySold = true; // #todayBay -wrap
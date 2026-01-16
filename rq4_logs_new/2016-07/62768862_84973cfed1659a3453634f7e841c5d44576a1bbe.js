var thumbs_up     = require('../../../images/emoji/thumbs_up.png');
var thumbs_down   = require('../../../images/emoji/thumbs_down.png');
var shitty        = require('../../../images/emoji/shitty.png');
var question      = require('../../../images/emoji/question.png');
var nope          = require('../../../images/emoji/nope.png');
var love_eyes_cat = require('../../../images/emoji/love_eyes_cat.png');
var interrobang   = require('../../../images/emoji/interrobang.png');
var celebration   = require('../../../images/emoji/celebration.png');
var appreciation  = require('../../../images/emoji/appreciation.png');
var hundred       = require('../../../images/emoji/hundred.png');

function emojiAssign(emoji) {
  switch (emoji) {
    case 'thumbs_up' :
      return thumbs_up;
    case 'thumbs_down' :
      return thumbs_down;
    case 'shitty' :
      return shitty;
    case 'question' :
      return question;
    case 'nope' :
      return nope;
    case 'love_eyes_cat':
      return love_eyes_cat;
    case 'interrobang' :
      return interrobang;
    case 'celebration' :
      return celebration;
    case 'appreciation' :
      return appreciation;
    case 'hundred' :
      return hundred;
    default :
      return thumbs_up
  }
}

export {emojiAssign};
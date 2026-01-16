import Popup from './Popup.js'

export default class PopupWithImage extends Popup {
  constructor (popupSelector) {
    super(popupSelector)
    this._popupImage = this._popup.querySelector('.popup__image');
    this._popupName= this._popup.querySelector('.popup__image-text');
  }

  open (cardLink, cardName) {
    this._popupImage.src = cardLink;
    this._popupName.textContent = cardName;
    this._popupImage.alt = cardName;
    super.open();
  }
}
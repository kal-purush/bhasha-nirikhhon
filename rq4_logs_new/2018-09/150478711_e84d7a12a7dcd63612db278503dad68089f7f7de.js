import {createBgLayer} from '@/util/cocos2d-util';

/** @class */
export class SpellCardChooser {
  /** @constructor */
  constructor() {
    // FIXME
    const spellCard1 = new cc.LabelTTF(
        '(9) バカ',
        'sans-serif',
        cc.winSize.height * 0.05
    );
    spellCard1.setFontFillColor(cc.color.BLACK);
    const spellCard1Bg = createBgLayer(spellCard1, cc.color(255, 192, 0));
    spellCard1Bg.setAnchorPoint(0, 1);
    spellCard1Bg.setPosition(cc.winSize.width * 0.55, cc.winSize.height * 0.3);

    this.node = spellCard1Bg;
  }

  /** @return {cc.Node} */
  getNode() {
    return this.node;
  }

  /**
   * @param {cc.Touch} touch
   * @param {cc.EventTouch} event
   * @return {*} TODO 発動したスペカの情報を返すべきだと思う
   */
  handleOnTouch(touch, event) {
    if (
      cc.rectContainsPoint(
          this.getNode().getBoundingBoxToWorld(),
          touch.getLocation()
      )
    ) {
      return true; // FIXME
    }
    return false;
  }
}
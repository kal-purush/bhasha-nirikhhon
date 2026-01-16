/// <reference path="../../node_modules/rfc6902/rfc6902.d.ts" />

import * as rfc6902 from "rfc6902";
import * as model from "../common/model";
import * as utils from "../common/utils";

export class BoardService {
  private shadow : model.Board;
  private current : model.Board;

  constructor(boardModel?: model.Board) {
    this.current = boardModel || {};
    this.shadow = utils.clone(this.current);
  }

  onClientConnection = () => {
    return { board: this.shadow };
  };

  onClientMessage = (msg: model.Message) => {
    var patch = (<model.PatchMessage>msg).patch;
    if (patch) {
      rfc6902.applyPatch(this.current, patch);
    }
  };

  onTic = () => {
    var changes = utils.clone(rfc6902.createPatch(this.shadow, this.current));
    if (changes.length) {
      rfc6902.applyPatch(this.shadow, changes);
      this.sendToClient(changes);
    }
  };

  sendToClient: (changes: any) => void;

}
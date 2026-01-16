/// <reference path="../../typings/knockout/knockout.d.ts" />
/// <reference path="../../typings/jasmine/jasmine.d.ts" />
/// <reference path="../../typings/socket.io-client/socket.io-client.d.ts" />

import { App } from "../common/client-app";
import * as viewModel from "../common/view-model";
import * as model from "../common/model";
import * as ko from "knockout";
import { BoardVM } from "../koclient/ko-view-model";


describe("Test boad client", function(){
  it("When a client recives a change on name, they are applied on the board", function(){
    // Arrange
    var board: model.Board = { name: "Old Name" };
    var boardVM: BoardVM = new BoardVM();
    boardVM.update(board);
    var client: App = new App(boardVM);
    var newName = "New Name";
    var change: model.PatchMessage = {patch: [{op: "add", path: "/name", value: newName}]};

    // Act
    client.onMessage(change);
    board = boardVM.toPlain();

    // Assert
    expect(board.name).toBeDefined();
    expect(board.name).toBe(newName);
    expect(true).toBe(true);
  });

  it("When a change it's applied on the client, they will be send this change", function(){
    // Arrange
    var board: model.Board = { name: "Old Name" };
    var boardVM: BoardVM = new BoardVM();
    boardVM.update(board);
    var client: App = new App(boardVM);
    var newName = "New Name";
    var newChange: any = { };
    client.sendToServer = (change) => {
      newChange = change;
    };

    // Act
    board.name = newName;
    boardVM.update(board);
    client.onInterval();

    // Assert
    expect(newChange).toEqual([{ op: "replace", path: "/name", value: "New Name" }]);
  });
});
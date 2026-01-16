/// <reference path="../../typings/jasmine/jasmine.d.ts" />
/// <reference path="../custom-typings/defaults.d.ts" />

import {BoardService} from "../server/BoardService";
import * as model from "../common/model";

describe("Test Board Service", function(){
  it("On a new connection with the server, it returns the board", function(){
    // Arrange
    var board: BoardService = new BoardService();

    // Act
    var result = board.onClientConnection();

    // Assert
    expect(result).not.toBeNull();
    expect(result.board).toBeDefined();
  });

  it("On a change on the board sended to server, the change is saved on Server board", function(){
    // Arrange
    var board: model.Board = {name: "old Name"};
    var boardService = new BoardService(board);
    var newName = "New Name";
    var change: model.PatchMessage = {patch: [{op: "replace", path: "/name", value: newName}]};

    // Act
    boardService.onClientMessage(change);

    // Assert
    expect(board.name).toBe(newName);
  });

});
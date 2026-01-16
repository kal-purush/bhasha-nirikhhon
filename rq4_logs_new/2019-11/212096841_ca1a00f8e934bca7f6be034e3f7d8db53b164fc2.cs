using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Tetfuza;

namespace TetfuzaTests
{
    [TestClass]
    public class BackendTests
    {
        TetfuzaBackend _game1;
        FuzaPiece _piece1;
        [TestInitialize]
        public void Initialize()
        {
            _game1 = new TetfuzaBackend();
            _piece1 = new FuzaPiece(FuzaType.SquareBlock);
        }
        [TestMethod]
        public void TetfuzaConstructorTest()
        {
            Assert.AreEqual(_game1.Board.Count, TetfuzaBackend.BOARD_HEIGHT);
            Assert.AreEqual(_game1.Board[0].Count, TetfuzaBackend.BOARD_WIDTH);
            Assert.IsTrue(_game1.Board[0].Contains(TetfuzaBackend.EMPTY_CHAR));
        }
        [TestMethod]
        public void FuzaPieceTest()
        {
            var piece2 = new FuzaPiece(FuzaType.SquareBlock);
            CollectionAssert.AreEqual(_piece1.Piece[FuzaPiece.PIECE_SIZE / 2], piece2.Piece[FuzaPiece.PIECE_SIZE / 2]);
            // Every piece should have a block in their center
            Assert.AreEqual(_piece1.Piece[FuzaPiece.PIECE_SIZE / 2][FuzaPiece.PIECE_SIZE / 2], FuzaPiece.FUZA_CHAR);
        }
        [TestMethod]
        public void SendInputTest()
        {
            _game1.SendInput(1, 1, false);
            _game1.SendInput(2, -2, true);
            _game1.SendInput(-2, 2, false);
            Assert.AreEqual(_game1.UserInputDirection, 1);
            Assert.AreEqual(_game1.UserInputRotation, 1);
            Assert.AreEqual(_game1.UserInputDown, false);

        }
    }
}
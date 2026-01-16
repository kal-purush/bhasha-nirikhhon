using BusinessLogic;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BusinessServices
{
    public class GameRoomManagement
    {
        public Domain.Boards GetBoardById(int idBoard)
        {

            Domain.Boards domainBoard = new Domain.Boards();
            using (var context = new CrosswordsContext())
            {
                BusinessLogic.Board foundBoard = new BusinessLogic.Board();
                var foundBoards = (from board in context.Boards
                              where board.idBoard == idBoard
                              select board)
                                .ToList();
                if (foundBoards.Count == 1)
                {
                    foundBoard = foundBoards.First();
                    domainBoard.idBoard = foundBoard.idBoard;
                    domainBoard.boardMatrix = foundBoard.boardMatrix;

                    foreach (BusinessLogic.WordsBoard businessWordBoard in foundBoard.WordsBoards)
                    {
                        Domain.WordsBoard domainWordBoard = new Domain.WordsBoard();
                        domainWordBoard.idBoard = idBoard;
                        domainWordBoard.xStart = businessWordBoard.xStart;
                        domainWordBoard.xEnd = businessWordBoard.xEnd;
                        domainWordBoard.yStart = businessWordBoard.yStart;
                        domainWordBoard.yEnd = businessWordBoard.yEnd;

                        Domain.Word domainWord = new Domain.Word();
                        domainWord.term = businessWordBoard.Word.word1;
                        domainWord.clue = businessWordBoard.Word.clue;
                        domainWordBoard.Word = domainWord;

                        domainBoard.WordsBoards.Add(domainWordBoard);
                    }
                }


            }
            return domainBoard;
        }
    }
}
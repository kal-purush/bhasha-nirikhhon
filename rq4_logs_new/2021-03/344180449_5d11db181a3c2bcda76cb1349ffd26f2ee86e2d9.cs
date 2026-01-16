using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using ChessWithCohorts.Models;

namespace ChessWithCohorts.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;

        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            ChessBoard board = new ChessBoard();
            King WKing = new King(true);
            WKing.CurrentLocation = board.Squares[3,3];
            IEnumerable<Move> MoveSet = WKing.GetValidMoves(board);
            int i = 0;
            Move MyMove = new Move();
            foreach (Move m in MoveSet)
            {
                i++;
                if(i == 3)
                {
                    board.MoveChessPiece(WKing, m);
                    // WKing.CurrentLocation = m.EndingLocation;
                    MyMove = m;
                }
            }
            var game = new {
                pieces = WKing,
                moves = MoveSet,
                mymove = MyMove,
                board = board,
            };

            return Json(game);
        }

        public IActionResult Privacy()
        {
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}
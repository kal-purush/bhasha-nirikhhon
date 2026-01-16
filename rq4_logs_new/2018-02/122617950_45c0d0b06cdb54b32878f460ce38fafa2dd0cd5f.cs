using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using System.Web;
using System.Web.Mvc;
using Game_shop.DataAccessLayer;
using Game_shop.Models;
using Game_shop.Models.DataModel;

namespace Game_shop.Controllers
{
    public class HomeController : Controller
    {
        // GET: Home
        public ActionResult Index()
        {
            var model = new Home();

            using (var context = new GameDbContext())
            {
                model.ListofGames = context.Games.ToList();
            }

            return View(model);
        }

        public ActionResult CreateGame()
        {
            return View();
        }

        [HttpPost]
        public ActionResult CreateGame(Game game)
        {
            game.Id = Guid.NewGuid();

            using (var context = new GameDbContext())
            {
                context.Games.Add(game);

                context.SaveChanges();
            }

            return RedirectToAction("Index");
        }

        [HttpPost]
        public ActionResult DeleteGame(Guid id)
        {
            using (var context = new GameDbContext())
            {
                var Game = context.Games.FirstOrDefault(x => x.Id == id);

                if (Game != null)
                {
                    context.Games.Remove(Game);
                    context.Entry(Game).State = EntityState.Deleted;
                    context.SaveChanges();
                }
            }

            return RedirectToAction("Index");
        }

        public ActionResult UpdateGame(Guid id)
        {
            Game Game;
            using (var context = new GameDbContext())
            {
                Game = context.Games.FirstOrDefault(x => x.Id == id);
            }

            return View(Game);
        }

        [HttpPost]
        public ActionResult UpdateGame(Game postGame)
        {
            using (var context = new GameDbContext())
            {
                var entity = context.Games.Find(postGame.Id);

                if (entity != null)
                {
                    context.Entry(entity).CurrentValues.SetValues(postGame);
                }
                context.SaveChanges();
            }

            return RedirectToAction("Index");
        }
    }
}
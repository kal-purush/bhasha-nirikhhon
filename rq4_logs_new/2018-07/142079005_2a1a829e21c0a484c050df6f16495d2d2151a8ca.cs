using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using KTB.Models;

using Microsoft.EntityFrameworkCore;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Http;


using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;


namespace KTB.Controllers{

    public class GameController : Controller{


        private static KTBContext _context;
        private Controller current_context;
        public static string URL = "https://opentdb.com/api.php?";
        public GameController(KTBContext context){
            _context = context;
            current_context = this;
        }
        public static TriviaData.RootObject data=null;

        public static Users dbUser = null;
        public static string dif = "";
        public static Games game = null;
        public static List<Questions> globalQuestions = null;
        public static int score = 0;

        public IActionResult PlayGround(){
            int? userID = HttpContext.Session.GetInt32("LogedUserID");
            if(userID != null){
                dbUser = _context.users.SingleOrDefault(u => u.id == userID);
                if(dbUser != null){
                    ViewBag.LogedUser = dbUser;
                    return View();
                }
            }
            return RedirectToAction("Index", "Home");
        }


        public async  Task<ActionResult> GenerateGame(string category, string difficulty, string type, int amount){

            // Console.WriteLine("*********************************");
            // Console.WriteLine(category);
            // Console.WriteLine(type);
            // Console.WriteLine(difficulty);
            // Console.WriteLine(amount);
            // Console.WriteLine("*********************************");
            int? userID = HttpContext.Session.GetInt32("LogedUserID");
            if(userID != null){
                dbUser = _context.users.SingleOrDefault(u => u.id == userID);
                if(dbUser != null){
                    ViewBag.LogedUser = dbUser;
                    score = 0;
                    ViewBag.amount = amount;
                    
                    game = new Games(){
                        status = "Open",
                        dificulty = dif,
                        User = dbUser
                    };
                    _context.Add(game);
                    // dbUser.Games.Add(game);
                    _context.SaveChanges();

                    string question_URL = URL + "amount=" + amount;
                    if(category != "any"){
                        question_URL += "&category=" + category;
                    }
                    if(difficulty != "any"){
                        question_URL += "&difficulty=" + difficulty;
                    }
                    // question_URL += "&difficulty=" + difficulty;
                    dif = difficulty;
                    if(type != "any"){
                        question_URL += "&type=" + type;
                    }
                    Console.WriteLine("*********************************");
                    Console.WriteLine(question_URL);
                    Console.WriteLine("*********************************");

                    SaveData(await GetRequest(question_URL));

                    return View("PlayGround");
                }
            }
            return RedirectToAction("Index", "Home");
        }


        public async Task<ActionResult> QuickGame(string category){
            await GenerateGame(category, "easy", "", 10);
            return View("PlayGround");
        }


        public IActionResult AddFavCategory(int categoryid){
            Console.WriteLine("********************************");
            Console.WriteLine(categoryid);
            Console.WriteLine("********************************");
            int? userID = HttpContext.Session.GetInt32("LogedUserID");
            if(userID != null){
            dbUser = _context.users.SingleOrDefault(u => u.id == userID);
                if(dbUser != null){
                    Categories category = _context.categories
                    .SingleOrDefault(cat => cat.id == categoryid);
                    
                    UserCategory uc = new UserCategory(){
                        Category = category,
                        User = dbUser
                    };
                    dbUser.UserCategory.Add(uc);
                    category.UserCategory.Add(uc);
                    _context.Add(uc);
                    _context.SaveChanges();
                }
            }
            return RedirectToAction("Index", "Home");
        }

        public IActionResult RemoveFavCategory(int id){
            UserCategory usercat = _context.usersCategory
            .SingleOrDefault(uc => uc.id == id);
            _context.Remove(usercat);
            _context.SaveChanges();
            return RedirectToAction("Index", "Home");
        }


        public IActionResult CheckAnswers(){

            int? userID = HttpContext.Session.GetInt32("LogedUserID");
            if(userID != null){
                dbUser = _context.users.SingleOrDefault(u => u.id == userID);
                if(dbUser == null){
                    return RedirectToAction("Index", "Home");
                }
            

                Console.WriteLine("***********************************");
                // int score = 0;
                // globalQuestions
                try{

                    foreach (string key in Request.Form.Keys){
                        string value = Request.Form[key];
                        int keyId;
                        if(int.TryParse(key, out keyId)){

                            Questions qest = null;
                            string user_answer = null;

                            foreach(var gq in globalQuestions){
                                if(gq.id == keyId){
                                    qest = gq;
                                }
                            }
                            foreach(var ans in qest.Answers){
                                if(ans.id == int.Parse(value)){
                                    user_answer = ans.answer;
                                    if(ans.correct_answer == 1){
                                        score += 100;
                                    }
                                }
                            }

                            UsersAnswers ua = new UsersAnswers(){
                                userId = dbUser.id,
                                questionId = qest.id,
                                answer = user_answer
                            };
                            _context.Add(ua);
                            // _context.SaveChanges();
                        }
                    }
                }catch(Exception e){
                    Console.WriteLine("*********************************" + e.ToString() + "*********************************");
                }
                finally{
                    game.status = "Closed";
                    dbUser.points += score;
                    // _context.Add(dbUser);
                    _context.SaveChanges();
                }
            }
            Console.WriteLine("***********************************");
            // return RedirectToAction("Index", "Home");
            return RedirectToAction("ViewResults");
        }

        public IActionResult ViewResults(){
            int? userID = HttpContext.Session.GetInt32("LogedUserID");
            if(userID != null){
                dbUser = _context.users.SingleOrDefault(u => u.id == userID);
                if(dbUser == null){
                    return RedirectToAction("Index", "Home");
                }

                List<Questions> allQuestions = _context.questions
                .Where(q => q.Game == game)
                .Include(q => q.Game)
                .Include(q => q.Answers)
                .Include(q => q.UsersAnswers)
                .ToList();

                ViewBag.User_Answer = allQuestions;
                ViewBag.earnedPoints = score;
                
                return View();
            }
            return RedirectToAction("Index", "Home");
        }
            
        async static Task<TriviaData.RootObject> GetRequest(string url){
            using(HttpClient client = new HttpClient()){
                using (HttpResponseMessage response = await client.GetAsync(url)){
                    using(HttpContent content = response.Content){
                        string mycontent = await content.ReadAsStringAsync();
                        TriviaData.RootObject data = Newtonsoft.Json.JsonConvert.DeserializeObject<TriviaData.RootObject>(mycontent);
                        return data;
                    }
                }
            }
        }


        public void SaveData(TriviaData.RootObject data){
            foreach(var d in data.results){
                Questions q = new Questions(){
                    question = d.question,
                    Game = game,
                }; 
                
                _context.Add(q);
                
                Answers answer = new Answers(){
                    answer = d.correct_answer,
                    correct_answer = 1,
                    Question = q
                };
                
                _context.Add(answer);
                _context.SaveChanges();
               
                foreach(var i_a in d.incorrect_answers){
                    Answers i_answer = new Answers(){
                        answer = i_a,
                        correct_answer = 0,
                        Question = q
                    };
                    _context.Add(i_answer);
                }
            }
            _context.SaveChanges();

            List<Questions> questions = _context.questions
            .Where(q => q.Game == game)
            .Include(q => q.Game)
            .ToList();
            foreach(var q in questions){
                q.Answers = q.Answers.OrderBy(a => Guid.NewGuid()).ToList();
            }
            globalQuestions = questions; 
            ViewBag.GameData = questions; 
        }
    }
}
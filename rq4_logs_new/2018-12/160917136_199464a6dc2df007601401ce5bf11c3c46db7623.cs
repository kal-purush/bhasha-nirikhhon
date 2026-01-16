using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using RestSharp;
using System.IO;
namespace ReportGenerators
{
    public class CanvasCourse
    {
        //https://canvas.instructure.com/doc/api/courses.html
        public int id { get; set; }
        public string name { get; set; }
        public string course_code { get; set; }
    }
    public class CanvasModule
    {
        //https://canvas.instructure.com/doc/api/modules.html
        public int id { get; set; }
        public string name { get; set; }
        public int items_count { get; set; }
        public string items_url { get; set; }
    }
    public class CanvasModuleItem
    {
        //https://canvas.instructure.com/doc/api/modules.html
        public int id { get; set; }
        public string title { get; set; }
        public string type { get; set; }
        public int content_id { get; set; }
        public string html_url { get; set; }
        public string url { get; set; }
        public string page_url { get; set; }
    }
    public class CanvasPage
    {
        //https://canvas.instructure.com/doc/api/pages.html
        public string url { get; set; }
        public string title { get; set; }
        public string body { get; set; }
    }
    public class CanvasDiscussionTopic
    {
        //https://canvas.instructure.com/doc/api/discussion_topics.html
        public int id { get; set; }
        public string title { get; set; }
        public string message { get; set; }
        public string html_url { get; set; }
    }
    public class CanvasAssignment
    {
        //https://canvas.instructure.com/doc/api/assignments.html
        public int id { get; set; }
        public string name { get; set; }
        public string description { get; set; }
        public string html_url { get; set; }

    }
    public class CanvasQuiz
    {
        //https://canvas.instructure.com/doc/api/quizzes.html
        public int id { get; set; }
        public string title { get; set; }
        public string description { get; set; }

    }
    public class CanvasQuizQuestionAnswers
    {
        //https://canvas.instructure.com/doc/api/quiz_questions.html
        public int id { get; set; }
        public string answer_text { get; set; }
        public string answer_comments { get; set; }
        public string html { get; set; }
        public string comments_html { get; set; }
    }
    public class CanvasQuizQuesiton
    {
        //https://canvas.instructure.com/doc/api/quiz_questions.html
        public int id { get; set; }
        public string question_name { get; set; }
        public string question_type { get; set; }
        public string question_text { get; set; }
        public List<CanvasQuizQuestionAnswers> answers { get; set; }
    }
    public static class CanvasApi
    {
        //Class to control interaction with the Canvas API
        //Token is needed to authenticate, will need to adjust this to be stored in a seperate file instead of in this code.
        private static string token = (new JavaScriptSerializer().Deserialize<Dictionary<string, string>>(File.ReadAllText((Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\CanvasApiCreds.json")))["Token"]);
        //THe base domain url for the API
        //BYU has 3 main domain names
        private static string domain = (new JavaScriptSerializer().Deserialize<Dictionary<string, string>>(File.ReadAllText((Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\CanvasApiCreds.json")))["BaseUri"]);
        public static string CurrentDomain = "";
        public static void ChangeDomain(string domain)
        {
            CurrentDomain = domain;
            switch (domain)
            {
                case "BYU Online": //BYU
                    File.Copy((Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\BYU_CanvasApiCreds.json"), 
                        (Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\CanvasApiCreds.json"), true);
                    break;
                case "BYU IS Test": //test
                    File.Copy((Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\TEST_CanvasApiCreds.json"),
                        (Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\CanvasApiCreds.json"), true);
                    break;
                case "BYU Master Courses":
                    File.Copy((Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\MASTER_CanvasApiCreds.json"),
                        (Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\CanvasApiCreds.json"), true);
                    break;
                case "Directory":
                    //Directory, nothing should be needed
                    break;
            }
        }
        public static void ResetApiCreds()
        {
            token = (new JavaScriptSerializer().Deserialize<Dictionary<string, string>>(File.ReadAllText((Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\CanvasApiCreds.json")))["Token"]);
            domain = (new JavaScriptSerializer().Deserialize<Dictionary<string, string>>(File.ReadAllText((Environment.GetFolderPath(Environment.SpecialFolder.MyDocuments) + @"\CanvasApiCreds.json")))["BaseUri"]);
        }
        public static CanvasCourse GetCanvasCourse(int course_id)
        {
            //Will send request for basic course information
            string url = $"{domain}/api/v1/courses/{course_id}?per_page=10000&access_token={token}";
            var restClient = new RestClient(url);
            var request = new RestRequest(Method.GET);
            //Will return single course object with parameters we want
            var response = restClient.Execute<CanvasCourse>(request);
            return response.Data;
        }
        public static List<CanvasModule> GetCanvasModules(int course_id)
        {
            //Request for all modules within a course
            string url = $"{domain}/api/v1/courses/{course_id}/modules?per_page=10000&access_token={token}";
            var restClient = new RestClient(url);
            var request = new RestRequest(Method.GET);
            //Returns a List of CanvasModule objects
            var response = restClient.Execute<List<CanvasModule>>(request);
            return response.Data;
        }
        public static List<CanvasModuleItem> GetCanvasModuleItems(int course_id, int module_id)
        {
            //Request for all items within a module
            string url = $"{domain}/api/v1/courses/{course_id}/modules/{module_id}/items?per_page=10000&access_token={token}";
            var restClient = new RestClient(url);
            var request = new RestRequest(Method.GET);
            //Returns a List of CanvasModuleItems
            var response = restClient.Execute<List<CanvasModuleItem>>(request);
            return response.Data;
        }
        public static CanvasPage GetCanvasPage(int course_id, string page_url)
        {
            //Request for a single canvas page
            string url = $"{domain}/api/v1/courses/{course_id}/pages/{page_url}?per_page=10000&access_token={token}";
            var restClient = new RestClient(url);
            var request = new RestRequest(Method.GET);
            var response = restClient.Execute<CanvasPage>(request);
            return response.Data;
        }
        public static CanvasDiscussionTopic GetCanvasDiscussionTopics(int course_id, int topic_id)
        {
            //Request for a signle discussion topic
            string url = $"{domain}/api/v1/courses/{course_id}/discussion_topics/{topic_id}?per_page=10000&access_token={token}";
            var restClient = new RestClient(url);
            var request = new RestRequest(Method.GET);
            var response = restClient.Execute<CanvasDiscussionTopic>(request);
            return response.Data;
        }
        public static CanvasAssignment GetCanvasAssignments(int course_id, int content_id)
        {
            //Request for a single canvas assignment page
            string url = $"{domain}/api/v1/courses/{course_id}/assignments/{content_id}?per_page=10000&access_token={token}";
            var restClient = new RestClient(url);
            var request = new RestRequest(Method.GET);
            var response = restClient.Execute<CanvasAssignment>(request);
            return response.Data;
        }
        public static CanvasQuiz GetCanvasQuizzes(int course_id, int content_id)
        {
            //Request for a single canvas quiz
            string url = $"{domain}/api/v1/courses/{course_id}/quizzes/{content_id}?per_page=10000&access_token={token}";
            var restClient = new RestClient(url);
            var request = new RestRequest(Method.GET);
            var response = restClient.Execute<CanvasQuiz>(request);
            return response.Data;
        }
        public static List<CanvasQuizQuesiton> GetCanvasQuizQuesitons(int course_id, int content_id)
        {
            //Request for the list of quiz questions (and answers) of a canvas quiz
            string url = $"{domain}/api/v1/courses/{course_id}/quizzes/{content_id}/questions?per_page=10000&access_token={token}";
            var restClient = new RestClient(url);
            var request = new RestRequest(Method.GET);
            var response = restClient.Execute<List<CanvasQuizQuesiton>>(request);
            return response.Data;
        }

    }
}
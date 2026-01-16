using System;
using static DataStructuresAndLINQ.Queries;

namespace DataStructuresAndLINQ
{
    class Program
    {
        static void Main()
        {
            var users = GetUsersInfo();
            /*string postsInfo= GetPostsInfo();
            Console.WriteLine("Posts: \n" + postsInfo);
            Console.WriteLine("Comments: \n" + GetCommentsInfo());
            Console.WriteLine("Todos: \n" + GetTodosInfo());
            Console.WriteLine("Address: \n" + GetAddressInfo());*/
            foreach (var user in users)
            {
                Console.WriteLine($"UserId: {user.Id }, userName: {user.Name}");
            }
            Console.ReadKey();
        }
        
    }
}
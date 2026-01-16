using UnityEngine;


namespace JevLogin
{
    public sealed class Lesson05Task02 : MonoBehaviour
    {
        private void Start()
        {
            var str = "Mama";
            Debug.Log(str.GetNumbers());
        }
    } 

    public static class ExtentionsLesson05Task02
    {
        public static int GetNumbers(this string str)
        {
            // В Си нет строк, но есть массив сиволов, и там бы я поступал так
            int result = 0;

            for (int i = 0; i < str.Length; i++)
            {
                result++;
            }
            //return result;

            // Но в C# можно сразу вернуть длину строки 
            return str.Length;
        }
    }
}
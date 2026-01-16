using System.Diagnostics;
using System.Reflection;
using static System.Net.Mime.MediaTypeNames;

internal class Program
{
    

    private static void Main(string[] args)
    {

        while (true)
        {
            int countQuestions = 5;

            int[] arrayCountQuestions = GetArrayCountQuestions(countQuestions);

            string[] questions = GetQuestions(countQuestions);

            int[] answers = GetAnswers(countQuestions);

            int countRightAnswers = 0;

            string[] diagnoses = GetDiagnoses(countRightAnswers);




            arrayCountQuestions = RandomizeNumbers(countQuestions, arrayCountQuestions);

            string[] randomQuestions = RandomizeQuestions(questions, arrayCountQuestions, countQuestions);

            int[] randomAnswers = RandomizeAnswers(answers, arrayCountQuestions, countQuestions);



            Console.WriteLine("Напишите Ваше Имя");
            string name = Console.ReadLine();
            //xcexwexwxxsxswxwdxsx

            for (int i = 0; i < countQuestions; i++)
            {



                Console.WriteLine("Вопрос # " + (i + 1));
                Console.WriteLine(randomQuestions[i]);


                int userAnswer = Convert.ToInt32(Console.ReadLine());
                int rightAnswer = randomAnswers[i];

                if (userAnswer == rightAnswer)
                {
                    countRightAnswers++;
                }
            }


            Console.WriteLine(name + ", Ваш диагноз - " + diagnoses[countRightAnswers]);


            string messageToUser = "Если желаете повторить - нажмите - ДА или НЕТ, если хотите выйти";


            bool userChoise = GetUserChoise(messageToUser);

            if (userChoise == false)
            {
                break;
            }

        }


    }

    static bool GetUserChoise(string message)
    {

        while (true)
        {
            Console.WriteLine(message);
            string userInput = Console.ReadLine();

            if (userInput.ToLower() == "да")
            {
                return (true);
            }

            if (userInput.ToLower() == "нет")
            {
                return (false);
            }
        }


    }


    static int[] GetArrayCountQuestions(int countQuestions)
    {
        int[] arrayCountQuestions = new int[countQuestions];


        for (int i = 0; i < countQuestions; i++)
        {

            arrayCountQuestions[i] = i + 1;

        }


        return (arrayCountQuestions);
    }

    static int[] RandomizeNumbers(int countQuestions, int[] arrayCountQuestions)
    {
        Random randomIndex = new Random();


        for (int i = countQuestions - 1; i >= 0; i--)
        {


            int j = randomIndex.Next(i);

            int tmpNumbers = arrayCountQuestions[j];
            arrayCountQuestions[j] = arrayCountQuestions[i];
            arrayCountQuestions[i] = tmpNumbers;




        }
        return (arrayCountQuestions);
    }




    static string[] GetQuestions(int countQuestions)
    {
        string[] questions = new string[countQuestions];
        questions[0] = "Сколько будет два плюс два умноженное на два?";
        questions[1] = "Бревно нужно распилить на 10 частей, Сколько нужно сделать распилов?";
        questions[2] = "На двух руках 10 пальцев  (Сколько пальцев на 5 руках?)";
        questions[3] = "Укол делают каждые пол часа  Сколько нужно минут для 3 уколов?";
        questions[4] = "Пять свечей сгорело  Две потухли  Сколько свечей осталось?";

        return (questions);
    }

    static int[] GetAnswers(int countQuestions)
    {
        int[] answers = new int[countQuestions];
        answers[0] = 6;
        answers[1] = 9;
        answers[2] = 25;
        answers[3] = 60;
        answers[4] = 2;
        return (answers);
    }


    static string[] GetDiagnoses(int countRightAnswers)
    {
        string[] diagnoses = new string[6];
        diagnoses[0] = "Идиот";
        diagnoses[1] = "Кретин";
        diagnoses[2] = "Дурак";
        diagnoses[3] = "Нормальный";
        diagnoses[4] = "Талант";
        diagnoses[5] = "Гений";
        return (diagnoses);

    }



    static string[] RandomizeQuestions(string[] questions, int[] arrayCountQuestions, int countQuestions)
    {


        string[] randomQuestions = new string[countQuestions];

        for (int i = 0; i < countQuestions; i++)
        {


            randomQuestions[i] = questions[arrayCountQuestions[i] - 1];

        }


        return (randomQuestions);
    }


    static int[] RandomizeAnswers(int[] answers, int[] arrayCountQuestions, int countQuestions)
    {


        int[] randomAnswers = new int[countQuestions];

        for (int i = 0; i < countQuestions; i++)
        {


            randomAnswers[i] = answers[arrayCountQuestions[i] - 1];

        }


        return (randomAnswers);
    }


}



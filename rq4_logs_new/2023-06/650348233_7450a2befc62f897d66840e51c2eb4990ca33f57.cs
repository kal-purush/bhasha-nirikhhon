using System;
using System.Security.Cryptography;
using System.Text;

public class Program
{
    public static void Main()
    {
        //task1();

        //task2();

        //task3();

        //task4();

        //task5();

        GameLive();

        //task7();

        //task8();
    }

    static void task1()
    {
        string str = "Hello, I'm Max";

        string reversedStr = ReverseString(str);
        Console.WriteLine(reversedStr);
    }
    public static string ReverseString(string str)
    {
        char[] chars = str.ToCharArray();
        int start = 0;
        int end = chars.Length - 1;

        while (start < end)
        {
            char temp = chars[start];
            chars[start] = chars[end];
            chars[end] = temp;
            start++;
            end--;
        }

        return new string(chars);
    }

    static void task2()
    {
        Console.Write("Enter the number: ");
        int input = int.Parse(Console.ReadLine());

        Syracuse(input);
    }

    static void Syracuse(int number)
    {
        Console.WriteLine(number);

        if (number == 1)
        {
            return;
        }
        else if (number % 2 == 0)
        {
            Syracuse(number / 2);
        }
        else
        {
            Syracuse(number * 3 + 1);
        }
    }

    static void task3()
    {
        Console.WriteLine("Invalid words: wo214rd1, w15rd2 and wo1347rd3.");
        string input = "This is the corrected text: c#, wo214rd1, is: the! w15rd2; wo1347rd3? best.";
        string[] forbiddenWords = { "wo214rd1", "w15rd2", "wo1347rd3" };

        string[] words = input.Split(' ', ',', '.', ':', ';', '!', '?');

        List<string> filteredWords = new List<string>();

        foreach (string word in words)
        {
            if (!forbiddenWords.Contains(word))
            {
                filteredWords.Add(word);
            }
        }

        string filteredText = string.Join(" ", filteredWords);

        Console.WriteLine(filteredText);
    }

    static void task4()
    {
        string inputChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        int length = 10;

        string randomString = GenerateRandomString(inputChars, length);
        Console.WriteLine(randomString);
    }

    static Random random = new Random();

    static string GenerateRandomString(string inputChars, int length)
    {
        StringBuilder sb = new StringBuilder();
        int charCount = inputChars.Length;

        for (int i = 0; i < length; i++)
        {
            int randomIndex = random.Next(0, charCount);
            char randomChar = inputChars[randomIndex];
            sb.Append(randomChar);
        }

        return sb.ToString();
    }

    static void task5()
    {
        int[] array = { 7, 2, 4, 8, 3, 5, 0, 1, 9 };

        int n = array.Length;
        int expectedSum = (n * (n + 1)) / 2;
        int actualSum = 0;

        for (int i = 0; i < n; i++)
        {
            actualSum += array[i];
        }

        int missingNumber = expectedSum - actualSum;

        Console.WriteLine("Hole in the array: " + missingNumber);
    }

    static int width = 10;
    static int height = 10;
    static bool[,] grid = new bool[width, height];
    static bool[,] nextGeneration = new bool[width, height];

    //---------------------------------------------------------------------------------------------------------
    static void GameLive()
    {
        // Заповнюємо початкову конфігурацію "живими" клітинами
        InitializeGrid();

        // Головний цикл ігор
        while (true)
        {
            PrintGrid();
            CalculateNextGeneration();
            if (IsGameOver())
            {
                Console.WriteLine("Игра завершена.");
                break;
            }
            Console.ReadLine(); // Додано для паузи між поколіннями (натисніть Enter, щоб перейти до наступного покоління)
        }
    }

    static void InitializeGrid()
    {

        // Вручну розставляємо "живі" клітини в початковій конфігурації

        grid[1, 4] = true;
        grid[2, 3] = true;
        grid[5, 1] = true;
        grid[7, 6] = true;

        // або рандомно
        //Random random = new Random();

        //for (int y = 0; y < height; y++)
        //{
        //for (int x = 0; x < width; x++)
        //{
        // grid[x, y] = (random.Next(2) == 0);
        // }
        // }
    }

    static void PrintGrid()
    {
        Console.Clear();
        for (int y = 0; y < height; y++)
        {
            for (int x = 0; x < width; x++)
            {
                if (grid[x, y])
                    Console.Write("X");
                else
                    Console.Write("1");
            }
            Console.WriteLine();
        }
    }

    static void CalculateNextGeneration()
    {
        // Код для розрахунку наступного покоління платформи гри

        for (int y = 0; y < height; y++)
        {
            for (int x = 0; x < width; x++)
            {
                int liveNeighbors = CountLiveNeighbors(x, y); // Підраховуємо кількість "живих" сусідів поточної клітини

                // Применяем правила игры
                if (grid[x, y])
                {
                    if (liveNeighbors == 2 || liveNeighbors == 3)
                        nextGeneration[x, y] = true; // Клітина продовжує жити
                    else
                        nextGeneration[x, y] = false; // Клітина вмирає
                }
                else // Поточна клітка "мертва"
                {
                    if (liveNeighbors == 3)
                        nextGeneration[x, y] = true; // Клітина оживає
                    else
                        nextGeneration[x, y] = false; // Клітина залишається мертвою
                }
            }
        }

        // Обновляем состояние площадки игры на основе рассчитанного следующего поколения
        Array.Copy(nextGeneration, grid, nextGeneration.Length);
    }

    static int CountLiveNeighbors(int x, int y)
    {
        int liveNeighbors = 0;

        // Перевіряємо стан усіх восьми сусідніх клітин
        for (int i = -1; i <= 1; i++)
        {
            for (int j = -1; j <= 1; j++)
            {
                if (i == 0 && j == 0)
                    continue;

                int neighborX = x + i;
                int neighborY = y + j;

                // Перевіряємо, що сусідні клітини в межах майданчика гри
                if (neighborX >= 0 && neighborX < width && neighborY >= 0 && neighborY < height)
                {
                    if (grid[neighborX, neighborY])
                        liveNeighbors++;
                }
            }
        }

        return liveNeighbors;
    }

    static bool IsGameOver()
    {
        // Перевіряємо умову завершення гри: відсутність «живих» клітин на майданчику
        for (int y = 0; y < height; y++)
        {
            for (int x = 0; x < width; x++)
            {
                if (grid[x, y])
                    return false; // Знайдена "жива" клітина, гра продовжується
            }
        }

        return true; // Всі клітини мертві, гра завершена
    }

    //---------------------------------------------------------------------------------------------------------

    static void task7()
    {
        string dnaSequence = "AAACCCCCGGTTGGGGG";
        string compressedSequence = Compress(dnaSequence);
        Console.WriteLine(compressedSequence); // Виведе "A3C5G2T2G5"

        string compressed = "A3C5G2T2G5";
        string decompressedSequence = Decompress(compressedSequence);
        Console.WriteLine(decompressedSequence); // Виведе "AAACCCCCGGTTGGGGG"
    }

    public static string Compress(string dnaSequence)
    {
        StringBuilder compressed = new StringBuilder();
        int count = 1;

        for (int i = 1; i < dnaSequence.Length; i++)
        {
            if (dnaSequence[i] == dnaSequence[i - 1])
            {
                count++;
            }
            else
            {
                compressed.Append(dnaSequence[i - 1]);
                compressed.Append(count);
                count = 1;
            }
        }

        // Додаємо останній символ та його кількість
        compressed.Append(dnaSequence[dnaSequence.Length - 1]);
        compressed.Append(count);

        return compressed.ToString();
    }

    public static string Decompress(string compressed)
    {
        StringBuilder decompressedSequence = new StringBuilder();
        char currentChar = compressed[0];

        for (int i = 1; i < compressed.Length; i++)
        {
            if (char.IsDigit(compressed[i]))
            {
                int count = int.Parse(compressed[i].ToString());
                decompressedSequence.Append(new string(currentChar, count));
            }
            else
            {
                currentChar = compressed[i];
            }
        }

        return decompressedSequence.ToString();
    }

    static void task8()
    {
        byte[] key = new byte[16]; // Задайте ключ тут або згенеруйте його

        string plaintext = "Hello, I'm Max!";

        byte[] encryptedBytes = Encrypt(plaintext, key);
        string encryptedText = Convert.ToBase64String(encryptedBytes);

        Console.WriteLine("Encrypted text: " + encryptedText);

        byte[] decryptedBytes = Convert.FromBase64String(encryptedText);
        string decryptedText = Decrypt(decryptedBytes, key);

        Console.WriteLine("Decoded text: " + decryptedText);
    }

    static byte[] Encrypt(string plaintext, byte[] key)
    {
        using (Aes aesAlg = Aes.Create())
        {
            aesAlg.Key = key;
            aesAlg.Mode = CipherMode.ECB;

            ICryptoTransform encryptor = aesAlg.CreateEncryptor(aesAlg.Key, aesAlg.IV);

            byte[] plaintextBytes = Encoding.UTF8.GetBytes(plaintext);
            byte[] encryptedBytes = encryptor.TransformFinalBlock(plaintextBytes, 0, plaintextBytes.Length);

            return encryptedBytes;
        }
    }

    static string Decrypt(byte[] ciphertext, byte[] key)
    {
        using (Aes aesAlg = Aes.Create())
        {
            aesAlg.Key = key;
            aesAlg.Mode = CipherMode.ECB;

            ICryptoTransform decryptor = aesAlg.CreateDecryptor(aesAlg.Key, aesAlg.IV);

            byte[] decryptedBytes = decryptor.TransformFinalBlock(ciphertext, 0, ciphertext.Length);
            string plaintext = Encoding.UTF8.GetString(decryptedBytes);

            return plaintext;
        }
    }
}
using System.Collections;
using System.Collections.Generic;
using UnityEngine;


public sealed class ExampleDictionary : MonoBehaviour
{
    public void Main()
    {
        var dict = new Dictionary<char, string>();

        dict.Add('r', "Roman");
        dict.Add('i', "Ira");
        dict.Add('v', "Victor");

        foreach (KeyValuePair<char, string> user in dict)
        {
            Debug.Log($"{user.Key} - {user.Value}");
        }

        dict['i'] = "Roman";    // Изменяем элемент с ключом i
        dict['t'] = "Roman";    // Добавляем элемент с ключом t

        foreach (KeyValuePair<char, string> user in dict)
        {
            Debug.Log($"{user.Key} - {user.Value}");
        }

        dict.Remove('i');   //  Deleting an element by key

        if (dict.ContainsKey('i'))  // Проверяем, имеется ли элемент с ключом i
        {
            var tempUser = dict['i'];   // Получаем элемент по ключу i
        }

        foreach (var user in dict.Values)
        {
            Debug.Log(user);
        }

        #region C# 5

        Dictionary<int, string> dictionary = new Dictionary<int, string>
        {
            {1,"Roman" },
            {2,"Ivan" },
            {3,"Igor" },
            {4,"Vova" }
        };

        #endregion

        #region C# 6

        Dictionary<int, string> dictionary2 = new Dictionary<int, string>
        {
            [1] = "Roman",
            [2] = "Roman",
            [3] = "Roman",
            [4] = "Roman"
        };

        #endregion

    }
}
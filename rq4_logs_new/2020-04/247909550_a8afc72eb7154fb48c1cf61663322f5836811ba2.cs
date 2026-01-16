using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public static class ArrayExtensions
{
	/// <summary>
	/// 第二引数分だけ第一引数の配列からランダム、重複なしで値を抽出した配列を返す
	/// </summary>
	/// <typeparam name="Type"></typeparam>
	/// <param name="types"></param>
	/// <param name="num"></param>
	/// <returns></returns>
	public static T[] Shuffle<T>(this T[] types, int num)
	{
		T[] returnTypes = new T[num];

		for (int i = 0, len = types.Length; i < num; i++, len--)
		{
			int rand = Random.Range(0, len);

			returnTypes[i] = types[rand];
			types[rand] = types[len - 1];
		}

		return returnTypes;
	}
}
using System;
using System.Collections;
using System.Collections.Generic;

namespace UnityPlayAround
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			Console.WriteLine ("Hello World!");
			MainClass mc = new MainClass ();
			foreach (var item in mc.SayHello()) {
				Console.WriteLine (item);
			}
			foreach (var item in FilterWithYield ()) {
				Console.WriteLine (item);
			}
			foreach (var item in FilterWithoutYield()) {
				Console.WriteLine (item);
			}
		}

		static List<int> GetInitialData() {
			return new List<int> (){ 1, 2, 3, 4 };
		}

		static IEnumerable<int> FilterWithoutYield() {
			List<int> result = new List<int> ();
			foreach (int i in GetInitialData()) {
				if (i > 2) {
					result.Add (i);
				}
			}
			return result;
		}

		static IEnumerable<int> FilterWithYield() {
			foreach (int i in GetInitialData()) {
				if (i > 2) {
					yield return i;
				}
			}
			yield break;
			Console.WriteLine ("code not execute here");
		}

		public IEnumerable<string> SayHello (){
			yield return "Hello";
			yield return "World";
			yield return "!";
			yield break;
		}
	}
}
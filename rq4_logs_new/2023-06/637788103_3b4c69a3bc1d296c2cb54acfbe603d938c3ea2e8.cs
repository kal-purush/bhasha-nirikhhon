using System;
using System.Collections;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using OpenCover.Framework.Model;
using UnityEngine;
using File = System.IO.File;

public class Finish : MonoBehaviour
{
	private void Start()
	{
		IEnumerable<string> lines = File.ReadLines("Assets/Scenes/UserInterface/Backup.txt");

		foreach (string line in lines)
		{
			if (line.Length == 0)
				return;
			string[] lineSplit = Regex.Split(line, ",");
			int id = Int32.Parse(lineSplit[0]);
			int character = Int32.Parse(lineSplit[1]);
			
			
		}
	}
}
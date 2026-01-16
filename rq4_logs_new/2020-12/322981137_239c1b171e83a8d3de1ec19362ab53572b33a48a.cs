using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using UnityEngine;
using KModkit;


public class DNAMutationScript : MonoBehaviour
{
	public KMAudio Audio;
    public KMBombInfo Bomb;
	public KMBombModule Module;
	
	public KMSelectable[] Buttons;
	public GameObject[] Strands;
	public AudioClip[] SFX;
	public Material[] DarkerMaterials;
	public TextMesh TopDisplay, BottomDisplay;
	public TextMesh[] TopDisplayNumber, BottomDisplayNumber;
	
	int[] Color = new int[9], Chemical = new int[9], StrandColors = new int[9], Focus = new int[9];
	string[] Letters = {"G", "C", "A", "T"}, Colors = {"#ff0000ff", "#ffff00ff", "#00ff00ff", "#0000ffff"}, RepresentingLetters = new string[9];
	string[][] Mutation = new string[][]{
		new string[] {"GACT", "CGAT", "TCGA", "GATC"},
		new string[] {"ACGT", "TGCA", "CTAG", "ATCG"},
		new string[] {"GTAC", "ATGC", "GCTA", "TGAC"},
		new string[] {"CGTA", "TACG", "CATG", "GCAT"}
	};
	bool Interactable = true;
	List<string> Text = new List<string>(), Answer = new List<string>();
	
	
	//Logging
    static int moduleIdCounter = 1;
    int moduleId;
    private bool ModuleSolved;
	
	void Awake()
	{
		moduleId = moduleIdCounter++;
		for (int i = 0; i < Buttons.Length; i++)
		{
			int Press = i;
			Buttons[i].OnInteract += delegate ()
			{
				ButtonPress(Press);
				return false;
			};
		}
	}
	
	void Start()
	{
		string[] Alphabreak = {"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"};
		string Serial = Bomb.GetSerialNumber();
		string Log4 = "Mutation numbers: ";
		for (int x = 0; x < 9; x++)
		{
			switch (x)
			{
				case 0:
				case 1:
				case 2:
				case 3:
				case 4:
				case 5:
					Focus[x] = Array.IndexOf(Alphabreak, Serial[x].ToString()) != -1 ? (Array.IndexOf(Alphabreak, Serial[x].ToString()) + 1) % 4 : Int32.Parse(Serial[x].ToString()) % 4;
					break;
				case 6:
					Focus[x] = Bomb.GetBatteryCount() % 4;
					break;
				case 7:
					Focus[x] = Bomb.GetIndicators().Count() % 4;
					break;
				case 8:
					Focus[x] = Bomb.GetPortCount() % 4;
					break;
				default:
					break;
			}
			Log4 += Focus[x].ToString();
		}
		
		string[] TheColors = {"Red", "Yellow", "Green", "Blue"};
		string Log1 = "Sequence shown on top: ", Log2 = "Colors of each letter: ", Log3 = "DNA strand colors: ", Log5 = "Correct new sequence (before color strand rules): ";
		for (int x = 0; x < 9; x++)
		{
			Color[x] = UnityEngine.Random.Range(0,4);
			Chemical[x] = UnityEngine.Random.Range(0,4);
			StrandColors[x] = UnityEngine.Random.Range(0,4);
			Strands[x].GetComponent<MeshRenderer>().material = DarkerMaterials[StrandColors[x]];
			TopDisplay.text += x != 8 ? "<color=" + Colors[Color[x]] + ">" + Letters[Chemical[x]] + "</color> " : "<color=" + Colors[Color[x]] + ">" + Letters[Chemical[x]] + "</color>";
			RepresentingLetters[x] = Mutation[Chemical[x]][Color[x]][Focus[x]].ToString();
			Log1 += Letters[Chemical[x]];
			Log2 += x != 8 ? TheColors[Color[x]] + ", " : TheColors[Color[x]];
			Log3 += x != 8 ? DarkerMaterials[StrandColors[x]].name + ", " : DarkerMaterials[StrandColors[x]].name;
			Log5 += RepresentingLetters[x];
		}
		Debug.LogFormat("[DNA Mutation #{0}] {1}", moduleId, Log1);
		Debug.LogFormat("[DNA Mutation #{0}] {1}", moduleId, Log2);
		Debug.LogFormat("[DNA Mutation #{0}] {1}", moduleId, Log3);
		Debug.LogFormat("[DNA Mutation #{0}] {1}", moduleId, Log4);
		Debug.LogFormat("[DNA Mutation #{0}] {1}", moduleId, Log5);
		
		for (int x = 0; x < 2; x++)
		{
			int[] NumberChoice = {3, 5};
			for (int y = 0; y < 2; y++)
			{	
				if (y == 0)
				{
					NumberChoice.Shuffle();
				}
				
				if (x == 0)
				{
					TopDisplayNumber[y].text = NumberChoice[y].ToString();
				}
				
				else
				{
					BottomDisplayNumber[y].text = NumberChoice[y].ToString();
				}
			}
		}
		
		string Log6 = "Correct new sequence (after color strand rules): ", Log7 = "";
		for (int x = 0; x < 9; x++)
		{
			switch (RepresentingLetters[x])
			{
				case "G":
					switch (StrandColors[x])
					{
						case 0:
							Answer.Add("C");
							break;
						case 1:
							Answer.Add("G");
							break;
						case 2:
							Answer.Add("T");
							break;
						case 3:
							Answer.Add("A");
							break;
						default:
							break;
					}
					break;
				case "C":
					switch (StrandColors[x])
					{
						case 0:
							Answer.Add("G");
							break;
						case 1:
							Answer.Add("C");
							break;
						case 2:
							Answer.Add("A");
							break;
						case 3:
							Answer.Add("T");
							break;
						default:
							break;
					}
					break;
				case "A":
					switch (StrandColors[x])
					{
						case 0:
							Answer.Add("T");
							break;
						case 1:
							Answer.Add("A");
							break;
						case 2:
							Answer.Add("C");
							break;
						case 3:
							Answer.Add("G");
							break;
						default:
							break;
					}
					break;
					
				case "T":
					switch (StrandColors[x])
					{
						case 0:
							Answer.Add("A");
							break;
						case 1:
							Answer.Add("T");
							break;
						case 2:
							Answer.Add("G");
							break;
						case 3:
							Answer.Add("C");
							break;
						default:
							break;
					}
					break;
				default:
					break;
			}
			Log6 += Answer[x];
			Log7 = Answer[x] + Log7;
		}
		Debug.LogFormat("[DNA Mutation #{0}] {1}", moduleId, Log6);
		
		if (TopDisplayNumber[0].text == BottomDisplayNumber[0].text)
		{
			Answer.Reverse();
			Debug.LogFormat("[DNA Mutation #{0}] The sequence numbers do not differ. The actual sequence is {1}", moduleId, Log7);
		}
	}
	
	void ButtonPress (int Press)
	{
		Audio.PlayGameSoundAtTransform(KMSoundOverride.SoundEffect.ButtonPress, transform);
		Buttons[Press].AddInteractionPunch(0.2f);
		if (!ModuleSolved && Interactable)
		{
			BottomDisplay.text += Text.Count() == 0 ? Buttons[Press].GetComponentInChildren<TextMesh>().text : " " + Buttons[Press].GetComponentInChildren<TextMesh>().text;
			Text.Add(Buttons[Press].GetComponentInChildren<TextMesh>().text);
			
			if (Text.Count() == 9)
			{
				Interactable = false;
				StartCoroutine(Inspecting());
				string AnswerSubmitted = "You submitted: ";
				for (int x = 0; x < 9; x++)
				{
					AnswerSubmitted += Text[x];
				}
				Debug.LogFormat("[DNA Mutation #{0}] {1}", moduleId, AnswerSubmitted);
			}
		}
	}
	
	IEnumerator Inspecting()
	{
		yield return new WaitForSecondsRealtime(0.1f);
		bool CorrectAnswer = true;
		for (int x = 0; x < 9; x++)
		{
			TopDisplay.text = "";	
			for (int y = x + 1; y < 9; y++)
			{
				TopDisplay.text += y != 8 ? "<color=" + Colors[Color[y]] + ">" + Letters[Chemical[y]] + "</color> " : "<color=" + Colors[Color[y]] + ">" + Letters[Chemical[y]] + "</color>";
			}
			BottomDisplay.text = x != 8 ? BottomDisplay.text.Remove(0,2) : BottomDisplay.text.Remove(0);
			if (CorrectAnswer && Text[x] != Answer[x])
			{
				CorrectAnswer = false;
			}
			Audio.PlaySoundAtTransform(SFX[0].name, transform);
			yield return new WaitForSecondsRealtime(0.1f);
		}
		
		if (CorrectAnswer == false)
		{
			Debug.LogFormat("[DNA Mutation #{0}] That was incorrect. The module striked.", moduleId);
			Module.HandleStrike();
			Text = new List<string>();
			for (int x = 0; x < 9; x++)
			{
				TopDisplay.text += x != 8 ? "<color=" + Colors[Color[x]] + ">" + Letters[Chemical[x]] + "</color> " : "<color=" + Colors[Color[x]] + ">" + Letters[Chemical[x]] + "</color>";
			}
			Interactable = true;
		}
		
		else
		{
			Debug.LogFormat("[DNA Mutation #{0}] That was correct. The module solved.", moduleId);
			Audio.PlaySoundAtTransform(SFX[1].name, transform);
			ModuleSolved = true;
			Module.HandlePass();
			for (int x = 0; x < 9; x++)
			{
				Strands[x].GetComponent<MeshRenderer>().material = DarkerMaterials[0];
			}
			
			for (int x = 0; x < 2; x++)
			{
				for (int y = 0; y < 2; y++)
				{	
					string Done = "DONE";
					Buttons[x*2 + y].GetComponent<MeshRenderer>().material.color = new Color (0f, 1f, 0f);
					Buttons[x*2 + y].GetComponentInChildren<TextMesh>().text = Done[x*2 + y].ToString();
					if (x == 0)
					{
						TopDisplayNumber[y].text = "";
					}
					
					else
					{
						BottomDisplayNumber[y].text = "";
					}
				}
			}
		}
	}
	
	//twitch plays
    #pragma warning disable 414
    private readonly string TwitchHelpMessage = @"To submit your answer on the module, use the command !{0} submit <9-character long genetic string>";
    #pragma warning restore 414
	
	string[] CommandLetters = {"A", "T", "G", "C"};
	char[] CommandCharacters = {'A', 'T', 'G', 'C'};
	
	IEnumerator ProcessTwitchCommand(string command)
    {
		string[] parameters = command.Split(' ');
		if (Regex.IsMatch(parameters[0], @"^\s*submit\s*$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant))
        {
			yield return null;
			if (!Interactable)
			{
				yield return "sendtochaterror You are unable to interact with the module currently. The command was not processed.";
				yield break;
			}
			
			if (parameters.Length != 2)
			{
				yield return "sendtochaterror Invalid parameter length. The command was not processed.";
				yield break;
			}
			
			if (parameters[1].Length != 9)
			{
				yield return "sendtochaterror Sequence sent does not have a length of 9. The command was not processed.";
				yield break;
			}
			
			if (!CommandCharacters.All(c => parameters[1].ToUpper().Contains(c)))
			{
				yield return "sendtochaterror Sequence contain an invalid character. The command was not processed.";
				yield break;
			}
			
			for (int x = 0; x < parameters[1].Length; x++)
			{
				Buttons[Array.IndexOf(CommandLetters, parameters[1][x].ToString())].OnInteract();
				yield return new WaitForSecondsRealtime(.1f);
			}
		}
	}
}
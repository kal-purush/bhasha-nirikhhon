using UnityEngine;
using System.Collections;
using UnityEngine.EventSystems;

public interface IMorseEventHandler {
	void KeyInput (char input);
	void SentenceInput(string sentence);
}
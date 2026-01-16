using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class FortuneCookie : MonoBehaviour {

	public Text showQuote;

	private List<string> quoteList;

	// Use this for initialization
	FortuneCookie()
	{
		quoteList = new List<string>();

		quoteList.Add ("test fortune");
		quoteList.Add ("test fortune1");
		quoteList.Add ("test fortune2");
		quoteList.Add ("test fortune3");
		quoteList.Add ("test fortune4");
		quoteList.Add ("test fortune5");
		quoteList.Add ("test fortune6");

	}

	public void generateFortune()
	{
		showQuote.text = quoteList [Random.Range (0, (quoteList.Capacity) - 1)];
	}
}
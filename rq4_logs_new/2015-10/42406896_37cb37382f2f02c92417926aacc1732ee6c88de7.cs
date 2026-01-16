using UnityEngine;
using System.Collections;

public class TwitterShare : MonoBehaviour 
{


	void Start () 
	{
	
	}
	

	void Update () 
	{
	
	}

	public void TwitterShareing()
	{
		Application.OpenURL("http://twitter.com/intent/tweet?text=" + "I just jumped a distance of " + scoreDistance.score + "! Think you can beat that? Then check out LilyLeap at " + "https://www.facebook.com/lilyleap?fref=nf" + "&amp;lang= en");
	}
}
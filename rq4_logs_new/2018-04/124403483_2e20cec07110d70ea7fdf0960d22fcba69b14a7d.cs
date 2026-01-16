/**************************************************************
* Purpose: 
**************************************************************/
using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;

public class LevelSelect : MonoBehaviour 
{
	/**********************************************************
	* Purpose: 
	**********************************************************/
	public void level1()
	{
		SceneManager.LoadScene("Bridge");
	}

	/**********************************************************
	* Purpose: 
	**********************************************************/
	public void level2()
	{
		SceneManager.LoadScene("Bridge2");
	}


}
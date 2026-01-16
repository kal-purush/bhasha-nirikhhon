using UnityEngine;
using System.Collections;

public class GameLogic : MonoBehaviour 
{
	public HeroController librarian;
	public HeroController student;

	public Transform libStartPosition;

	public static GameLogic Logic;

	public int RoundNum;

	void Start () 
	{
		RoundNum = 1;

		librarian.transform.position = libStartPosition.position;
		
		student = GetComponent<StudentsController>().RandomStudentController();
		
		librarian.SetPlayer (0);
		student.SetPlayer (1);

		Controls.Functions.BlockPlayer (librarian.Player);
		Controls.Functions.BlockPlayer (student.Player);

		if (!Logic) 
		{
			Logic = this;
		}

		GetComponentInChildren<ScreenMessages> ().ShowRound1 ();
		StartCoroutine (StartGame ());
	}

	private IEnumerator StartGame()
	{
		yield return new WaitForSeconds (3);
		
		Controls.Functions.UnblockPlayer (librarian.Player, 1);
		Controls.Functions.UnblockPlayer (student.Player, 0);
		GetComponentInChildren<SessionTimer> ().SetSessionTime (120);
	}
	
	void Update () 
	{
		if (Input.GetKey(KeyCode.Escape))
		{
			Application.LoadLevel("MainMenu");
		}
	}

	public void CatchStudent(int player)
	{
		GetComponentInChildren<GameScore> ().AddScore (player);
		Controls.Functions.BlockPlayer (0);
		Controls.Functions.BlockPlayer (1);
	}

	public void NextStudent()
	{
		student = GetComponent<StudentsController>().RandomStudentController();
		student.SetPlayer (1 - librarian.Player);
		
		Controls.Functions.UnblockPlayer (librarian.Player, 1);
		Controls.Functions.UnblockPlayer (student.Player, 0);
	}

	public void ChangeRoles()
	{
		if (RoundNum < 2) 
		{
			GetComponent<StudentsController> ().NewRound();
			student = GetComponent<StudentsController> ().RandomStudentController ();

			librarian.SetPlayer (1 - librarian.Player);
			student.SetPlayer (1 - librarian.Player);

			librarian.transform.position = libStartPosition.position;
			librarian.transform.localScale = new Vector3(-1, 1, 1);
			
			StartCoroutine(NewRound());
		}
		else 
		{
			StartCoroutine(CountScore());
		}
	}

	private IEnumerator NewRound()
	{
		yield return new WaitForSeconds (1);

		if (GetComponentInChildren<ScreenMessages> ().isShowing ()) 
		{
			StartCoroutine(NewRound());
		} 
		else 
		{
			RoundNum++;
			Controls.Functions.UnblockPlayer (librarian.Player, 1);
			Controls.Functions.UnblockPlayer (student.Player, 0);
			
			GetComponentInChildren<SessionTimer>().SetSessionTime(120);
		}
	}

	public IEnumerator CountScore()
	{
		yield return new WaitForSeconds(3);

		if (FindObjectOfType<ScreenMessages> ().ShowBlackScreen ()) 
		{
			yield return new WaitForSeconds(0.5f);
		}

		GetComponentInChildren<GameScore> ().ShowResults ();
	}
}



















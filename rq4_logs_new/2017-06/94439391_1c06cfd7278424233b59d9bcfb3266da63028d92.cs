using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.Networking;

public class APIInterface : MonoBehaviour {

	// Use this for initialization
	void Start () {

		
	}

    public void GetExercises()
    {
        string exercise_url_attribute = "https://super-confusing-baby.herokuapp.com/exercises/1.json";
        StartCoroutine(GetJSON_Exercises(exercise_url_attribute));

        string steps_url_attribute = "https://super-confusing-baby.herokuapp.com/exercises/1/steps.json";
        StartCoroutine(GetJSON_Steps(steps_url_attribute));

        string commands_url_attribute = "https://super-confusing-baby.herokuapp.com/exercises/1/steps/1/commands.json";
        StartCoroutine(GetJSON_Steps(commands_url_attribute));
    }
    

    IEnumerator GetJSON_Exercises(string url)
    {
        UnityWebRequest www = UnityWebRequest.Get(url);

        yield return www.Send();

        if (www.isError)
        {
            Debug.Log(www.error);
        }
        else
        {
            Debug.Log(www.downloadHandler.text);
            ExerciseAPI exercise = JsonUtility.FromJson<ExerciseAPI>(www.downloadHandler.text);

            //Player[] player = JsonHelper.FromJson<Player>(jsonString);
            //ExerciseAPI e = JsonHelper.FromJson<ExerciseAPI>(www.downloadHandler.text);

            Debug.Log("Exercise: " + exercise.name);
            Debug.Log("Description: " + exercise.description);
            Debug.Log("Difficulty: " + exercise.difficulty);

        }
    }
    IEnumerator GetJSON_Steps(string url)
    {
        UnityWebRequest www = UnityWebRequest.Get(url);

        yield return www.Send();

        if (www.isError)
        {
            Debug.Log(www.error);
        }
        else
        {
            Debug.Log(www.downloadHandler.text);
            //StepAPI Step = JsonUtility.FromJson<StepAPI>(www.downloadHandler.text);
            StepAPI[] step = JsonHelper.FromJson<StepAPI>(www.downloadHandler.text);


            //Debug.Log("StepID: " + step[0].id);
            //Debug.Log("Instruction: " + step[0].instruction);
            //Debug.Log("response: " + step[0].correct_response);
            //Debug.Log("StepID: " + Step.id);
            //Debug.Log("Instruction: " + Step.instruction);
            //Debug.Log("response: " + Step.correct_response);
        }
    }
    IEnumerator GetJSON_Commands(string url)
    {
        UnityWebRequest www = UnityWebRequest.Get(url);

        yield return www.Send();

        if (www.isError)
        {
            Debug.Log(www.error);
        }
        else
        {
            Debug.Log(www.downloadHandler.text);
            //CommandAPI Command = JsonUtility.FromJson<CommandAPI>(www.downloadHandler.text);
            CommandAPI[] Command = JsonHelper.FromJson<CommandAPI>(www.downloadHandler.text);
            Debug.Log("CommandID: " + Command[0].id);
            Debug.Log("Command name: " + Command[0].name);
            Debug.Log("Command url: " + Command[0].url);

        }
    }
    // Update is called once per frame
    void Update () {
		
	}
}
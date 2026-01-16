using System.Collections;
using System.Collections.Generic;
using System.Linq;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

public class HomepageManager : MonoBehaviour
{

    public Button subjectButtonPrefab;
    public Transform buttonParent;
    private DatabaseManager databaseManager;

    // Start is called before the first frame update
    void Start()
    {
        databaseManager = FindObjectOfType<DatabaseManager>();

        var subjects = databaseManager.ExecuteQueryWithReturn<Subject>(Queries.GetAllSubjects);
        
        foreach(var subject in subjects)
        {
            var button = Instantiate(subjectButtonPrefab, buttonParent);
            button.GetComponentInChildren<Text>().text = subject.SubjectName;
            button.onClick.AddListener(() => LoadSubjectPage(subject.Id));
        }
    }

    private void LoadSubjectPage(long subjectId)
    {
        PlayerPrefs.SetInt("SubjectId", (int)subjectId);
        SceneManager.LoadScene("Subject");
    }
}
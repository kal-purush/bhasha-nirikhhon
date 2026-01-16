using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;
using NoteEditor.DTO;
using NoteEditor.Notes;
using Config;
using Monochrome_Notes;

public class MainManager : MonoBehaviour
{

    private float deltaTime;
    private float noteSpeed = 1;

    private string musicName = "tutorial"; //テストで入れてるけどほんとは曲選択画面からもらう

    [SerializeField] private GameObject noteObj;
    [SerializeField] private GameObject judgeLineObj;

    private GameMaster gameMaster;
    private MusicDTO.EditData editData;
    private MusicDTO.Note note;

    private static List<MyNote.NotePos> NOTES_LIST = new List<MyNote.NotePos>();
    private static readonly Dictionary<LineNum, float> LINE_POSITION = new Dictionary<LineNum, float>(){
        {LineNum.Line1, -3.75f},
        {LineNum.Line2, -1.25f},
        {LineNum.Line3, 1.25f},
        {LineNum.Line4, 3.75f}
    };

    // Use this for initialization
    void Start()
    {
        //gameMaster = GameObject.FindGameObjectWithTag("GameMaster").GetComponent<GameMaster>();
        //musicName = gameMaster.GetMusicName();
        //deltaTime = gameMaster.GetTimeDeltaTime();
        //noteSpeed = gameMaster.GetNoteSpeed();

        string json = Resources.Load<TextAsset>(musicName).ToString();
        editData = JsonUtility.FromJson<MusicDTO.EditData>(json);
        note = JsonUtility.FromJson<MusicDTO.Note>(json);

        for (int i = 0; i < note.notes.Count; i++)
        {
            float carrentTime = 60f / (editData.BPM * note.notes[0].LPB) * note.notes[i].num;
            NOTES_LIST.Add(new MyNote.NotePos(carrentTime, (LineNum)Enum.ToObject(typeof(LineNum), note.notes[i].block)));
            Debug.Log(NOTES_LIST[i].notesTimeg.ToString() + NOTES_LIST[i].lineNum);
        }

        Initialize();
    }

    // Update is called once per frame
    void Update()
    {

    }

    void Initialize()
    {
        foreach (var data in NOTES_LIST)
        {
            var obj = Instantiate(noteObj, transform);
            var note = obj.GetComponent<MyNote>();
            note.Intialize(data);
            var pos = obj.transform.position;
            pos.x = LINE_POSITION[note.notePos.lineNum];
            pos.y = judgeLineObj.transform.position.y + note.notePos.notesTimeg * noteSpeed;
            note.transform.position = pos;
        }

    }
}
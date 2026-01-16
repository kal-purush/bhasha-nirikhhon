using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class SelectManager : MonoBehaviour
{
    private const float SongListSpace = 240f;
    private const float SongScrollTime = 0.1f;
    
    [SerializeField] private Transform songsParentTransform;
    [SerializeField] private GameObject songPrefab;
    // [SerializeField] private Something with current song

    [Space(10)]
    [SerializeField] private Sprite[] rankImages;   // TODO: Add rank images
    [SerializeField] private Image rankImage;
    [SerializeField] private Text highScoreText;
    [SerializeField] private Text patternTypeText;

    private readonly List<SongData> _songList = new();
    private int _currentIndex;
    
    private PatternType _currentPatternType;
    private int _currentPatternDifficulty;

    private bool _songMoving;
    
    private void Start()
    {
        _currentIndex = 0;
        _currentPatternType = PatternType.Easy;
        _songMoving = false;
        
        // Song list initialization
        _songList.Clear();
        
        foreach (var song in SongListData.SongList)
        {
            _songList.Add(song);
        }

        // Song list UI initialization
        foreach (Transform t in songsParentTransform)
        {
            Destroy(t.gameObject);
        }

        for (int i = 0; i < _songList.Count; i++)
        {
            var song = _songList[i];
            var instance = Instantiate(songPrefab, songsParentTransform);
            var position = instance.transform.localPosition;
            position.y = -i * SongListSpace;
            instance.transform.localPosition = position;
            instance.transform.GetChild(0).GetComponent<Text>().text = song.SongName;
        }
        
        SetCurrentSongUI();
        SetCurrentPatternUI();
    }

    private void Update()
    {
        // Song scroll
        if (!_songMoving)
        {
            switch (Input.GetKeyDown(KeyCode.UpArrow), Input.GetKeyDown(KeyCode.DownArrow))
            {
                case (true, false):
                    MoveUp();
                    break;
                case (false, true):
                    MoveDown();
                    break;
            }
        }
        
        // Pattern type select
        if (Input.GetKeyDown(KeyCode.Tab))
        {
            SwitchPatternType();
        }
    }

    private void SetCurrentSongUI()
    {
        // TODO: Set current song UI
    }

    private void SetCurrentPatternUI()
    {
        var score = 1010000;    // TODO: Get score
        
        _currentPatternDifficulty = _songList[_currentIndex].Difficulty[(int)_currentPatternType];
        
        var patternText = _currentPatternType switch
        {
            PatternType.Easy => "Easy",
            PatternType.Hard => "Hard",
            PatternType.Vex  => "Vex",
            _ => throw new System.ArgumentException()
        };

        patternText += " " + _currentPatternDifficulty;
        
        patternTypeText.text = patternText;
        
        rankImage.sprite = rankImages[(int)GameManager.GetRank(score)];
        highScoreText.text = score.ToString();
    }

    private void MoveUp()
    {
        if (_currentIndex == 0) return;
        _currentIndex--;
        StartCoroutine(MoveSongCoroutine(true));
    }

    private void MoveDown()
    {
        if (_currentIndex == _songList.Count - 1) return;
        _currentIndex++;
        StartCoroutine(MoveSongCoroutine(false));
    }

    private IEnumerator MoveSongCoroutine(bool up)
    {
        _songMoving = true;
        var elapsedTime = 0f;
        
        var targetPosition = songsParentTransform.position + new Vector3(0f, up ? -SongListSpace : SongListSpace);
        while (elapsedTime < SongScrollTime)
        {
            songsParentTransform.position = Vector3.Lerp(songsParentTransform.position, targetPosition, elapsedTime / SongScrollTime);
            elapsedTime += Time.deltaTime;
            yield return null;
        }
        
        songsParentTransform.position = targetPosition;
        
        SetCurrentSongUI();
        SetCurrentPatternUI();
        _songMoving = false;
    }

    private void SwitchPatternType()
    {
        _currentPatternType = _currentPatternType switch
        {
            PatternType.Easy => PatternType.Hard,
            PatternType.Hard => PatternType.Vex,
            PatternType.Vex => PatternType.Easy,
            _ => throw new System.ArgumentException()
        };
        
        SetCurrentPatternUI();
    }

    public void OnClickPatternSelectButton()
    {
        SwitchPatternType();
    }

    public void OnClickSelectBackButton()
    {
        // TODO: Back to Main Scene
    }
}
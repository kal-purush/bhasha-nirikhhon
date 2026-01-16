using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System.IO;


public class PuzzleManager : Singleton<PuzzleManager>
{
    private ST_PuzzleDisplay puzzle;

    public override void Init()
    {
        transform.Find("SlidePuzzle").TryGetComponent<ST_PuzzleDisplay>(out puzzle);
        puzzle.gameObject.SetActive(false);
    }

    [ContextMenu("text")]
    public void Text() {
        StartPuzzle(Resources.Load<Texture>("Image/Album/emptyLeft"), 1);
    }


    /// <summary>
    /// 퍼즐 시작해주는 함수.
    /// </summary>
    public void StartPuzzle(Texture texture,int index) {
        //내부 함수 실
        puzzle.StartPuzzle(texture,index);

    }
}
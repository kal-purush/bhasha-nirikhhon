using UnityEngine;
using System.Collections;

public class BookManager : Singleton<BookManager>
{
    private Book book;
    private Sprite[] empty = new Sprite[2];
    private Sprite[] fillImage = new Sprite[8];
    private int maxBookSize = 8;


    public override void Init()
    {

        Transform albumTransform = LobbyManager.Instance.ObjectDictionary["Album"].transform;
        //albumTransform.Find("AutoFlip/Book").TryGetComponent<AutoFlip>(out autoFlip);
        albumTransform.Find("AutoFlip/Book").TryGetComponent<Book>(out book);
        Setting();
        SettingBookImage();
    }

    /// <summary>
    /// 기본 세팅
    /// </summary>
    private void Setting() {
        empty[0] = Resources.Load<Sprite>("Image/Album/emptyLeft");
        empty[1] = Resources.Load<Sprite>("Image/Album/emptyRight");
        book.bookPages = new Sprite[maxBookSize];
        fillImage = new Sprite[maxBookSize];
    }

    /// <summary>
    /// 북 이미지 넣기
    /// </summary>
    private void SettingBookImage()
    {
        for (int i = 0; i < maxBookSize; i++)
        {
            book.bookPages[i] = (fillImage[i] != null) ? fillImage[i] : empty[i % 2];
        }
    }

    /// <summary>
    /// 북에 맞는 이미지 넣어주는 함수
    /// </summary>
    /// <param name="index">이미지의 위치</param>
    /// <param name="sprite">이미지 Sprite</param>
    public void SetBookImage(int index , Sprite sprite) {
        book.bookPages[index] = sprite;
        fillImage[index] = sprite;
    }
}
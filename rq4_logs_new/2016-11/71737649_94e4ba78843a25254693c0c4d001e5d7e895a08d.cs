using UnityEngine;
using System.Collections;
using System.Collections.Generic;
using System;
using System.IO;
using UnityEngine.UI;
public class MusicData_
{

    public string Title;
    public string Subtitle;
    public string Artist;
    public double Bpm;
    public string MusicAddress;
    public string Jackect;
    public int SelectedDifficult;
    public int[] Difficulty = new int[4];
    public string MovieAddress;
    public double Offset;
    public double SelectOffset;
    public double Selectlong;
    public string[] Notes = new string[4];
    public double HiSpeed;
    //public int[,,] NotesSet = new int[300, 12, 5]; //ノーツ保存庫
    public List<Musicnote> NoteList = new List<Musicnote>();
    public List<int> Line = new List<int>(); // [BeatPerMeasure]の保存庫
    public List<double> BMSArr = new List<double>();   //各拍のBMSカウントを算出する
   // public List<LongArr> LongNotes = new List<LongArr>();   //各拍のロングノーツのBMSカウントを取得する
    public List<LongArr> LongNotesList = new List<LongArr>(); //処理したロングノーツを格納する
    public double DeffBPM;
};

public class Musicnote
{
    public int[,] NotesSet = new int[12, 5];   //ノーツ保管庫
    public int[] Option = new int[3];    //変則等保存庫
}

/// <summary>
/// ロングノーツを拍ごとに格納
/// </summary>
public class LongArr {
    /// <summary>
    /// ロングノーツの開始カウントと終了カウントを格納する
    /// </summary>
    public int Keynum;
    public int CutBeat; //何分で刻むかを格納
    public int Count;   //何拍分かを格納
    public int NowCount; //現在何拍目であるかを格納
    public double StartLineCount;   //ロングノーツがどのカウントで開始しているかを格納
    public double EndLineCount;     //ロングノーツがどのカウントで終了するかを格納
}

public class GlobalValue : MonoBehaviour
{
    public MusicData_ MusicParam;
    private GameObject movie;
    public int MaxNotes = 0;
    public double BMSCount = 0;
    //double FPS = 60;
    public double Multi = 10000;  //1小節を何カウントに分解するかの分解能の設定
    int NowBeat = 0;
    //double DeffBPM;
    bool endflag = false;
    double FPS = 60.0;
    bool OffsetFlag = false;
    public float Offset = 0;
    double temp =0;
    public Text MusicName;
    public GameObject JacketPlane;
    int LongCheckCount = 0;
    // Use this for initialization
    /*void Awake()
    {
        MusicParam = new MusicData_();
        this.GetComponent<XMLLoader>().OpenXml();
        movie = GameObject.Find("MoviePlaneSub");
        movie.GetComponent<GetMovieFileFromLocal>().StreamPlayVideoAsTexture(MusicParam.MovieAddress, MusicParam.MusicAddress);
        MusicParam.SelectedDifficult = 3;
        string[] AnalyzeString = MusicParam.Notes;
        int Difficult = MusicParam.SelectedDifficult;
        AnalyzingNotes(AnalyzeString[Difficult]);
        MusicParam.HiSpeed = 6;
        CalcBMSCount();
        CalcLongBMSCount();
        MusicName.text = MusicParam.Title;
        //JacketPlane.GetComponent<SetPicture>().setpic(System.IO.Directory.GetCurrentDirectory() + MusicParam.Jackect);
        //StartCoroutine(GetJacketImage(JacketPlane, MusicParam.Jackect));
        BMSCount = -Multi;
    }*/
    IEnumerator GetJacketImage(GameObject gameObject, string filePath)
    {
        WWW file = new WWW("file://" + System.IO.Directory.GetCurrentDirectory() + filePath);
        yield return file;
        gameObject.GetComponent<Renderer>().material.mainTexture = file.texture;
    }
    // <summary>
    // ノーツを解析する
    // </summary>
    // <param name="analyze">解析する文字列</param>
    public void AnalyzingNotes(string analyze)
    {
        int k = 0;  //多次元配列格納用
        int linepos = 1;    //何拍目にいるか
        int notepos = 0;    //譜面のどのブロックを見ているか
        //int l = 0;          //Option位置格納変数
        //int m = 0;          //Line位置格納用変数
        int befline = 0;    //全小節の位置を格納
        MusicParam = new MusicData_();
        bool spaceflag = false; //空白(' ')を発見したら、次の'|'まで読み込みを停止するフラグ
        Musicnote buff = new Musicnote();
        StreamWriter log = new StreamWriter(@"D:\Music_Editor\log.txt", true);
        //Debug.Log("analyze[130] is "+"["+analyze[130]+"]");
        Debug.Log("analyze lenghth is "+analyze.Length);
        for (int i = 0; i < analyze.Length; i++)
        {
           
            if (analyze[i]== ' ')
            {
                //Debug.Log("analyze[" + i + "] is space");
                spaceflag = true;   //このフラグが起動中は譜面配列化を停止する
                continue;
            }
            if (analyze[i] == '|')
            {
                //Debug.Log("analyze["+i+"] is |");
                    spaceflag = false; //読み込みフラグ解除
                notepos++;
                notepos %= 14;
            }
            else if (analyze[i] == '@')           //小節の終わり
            {
                MusicParam.Line.Add(linepos-befline);      //１小節の拍数を格納する
              // Debug.Log("MusicParam.Line.add");
                befline = linepos;
            }
            else if ( analyze[i] == '\n')
            {
                //Debug.Log("analyze[" + i + "] is \\n");

                linepos++;
                
                MusicParam.NoteList.Add(buff);
                
                //Debug.Log("clear!");
                //       Debug.Log("MusicParam.NoteList.add");
                buff = new Musicnote();
            }
            else if(analyze[i] == '\r')
            {
                //Debug.Log("analyze[" + i + "] is \\r");
                continue;
            }
            
            else {
                if (spaceflag == false)
                {
                    if (analyze[i] == ';' || analyze[i] == ',')      //拍の終わり
                    {
                       // Debug.Log("analyze[" + i + "] is ;");
                    }
                    else if (notepos < 2)       //変則読み込みモード
                    {
                        //MusicParam.NoteList[l].Option[k] = ConvertStringToInt(ref i,analyze);
                        buff.Option[k] = ConvertStringToInt(ref i,analyze);
                        //Debug.Log(buff.Option[k]);
                        
                        k++;
                        k %= 3;
                    }
                    else
                    {
                        if (notepos <= 13)
                        {
                            
                            buff.NotesSet[notepos - 2, k] = ConvertStringToInt(ref i, analyze);
                            //log.Write(buff.NotesSet[notepos - 2, k]);
                            if (k == 0)
                            {
                                
                                if (Char.IsNumber(analyze[i])&&analyze[i] !='0')
                                {
                                    //Debug.Log("Char.IsNumber()");
                                    MaxNotes++;
                                }
                            }
                            //MusicParam.NoteList[linepos].NotesSet[notepos - 2, k] = ConvertStringToInt(ref i,analyze);   //格納している拍,レーン位置,各データの位置に値を格納
                            k++;
                            k %= 5;
                        }else
                        {
                        }
                    }
                    /*for(int w=0;w< MusicParam.NoteList.Count;w++)
                    {
                        log.WriteLine(MusicParam.NoteList);
                    }*/
                    
                    //log.Write("\n");
                }
            }
            
        }

        Debug.Log(MusicParam.NoteList.Count);

        log.Flush();
        log.Close();

    }

    int  ConvertStringToInt(ref int i,string analyze)
    {
        string setstr = "";
        for(int j = i;j < analyze.Length; j++)
        {
            if(char.IsNumber(analyze, j))
            {
                setstr += analyze[j];
                i = j;
            }else
            {
                break;
            }
        }
        return Convert.ToInt32(setstr.ToString());
    }
    /// <summary>
    /// BMSカウントを算出する
    /// </summary>
    void CalcBMSCount()
    {
        List<int> CalcMeasureLine_Calc = MusicParam.Line;
        List<Musicnote> OptionData_Calc = MusicParam.NoteList;
        double DeffBPM_Calc = MusicParam.Bpm;
        double NowBPM_Calc = DeffBPM_Calc;
        int NowBeat_Calc = 0;
        double NowBMSCount_Calc = 0;
        LongArr bufflong;
        List<LongArr> QueueLongList = new List<LongArr>(); //処理すべきロングノーツをスタックに格納する
        //小節ごとにBMSカウントを割り当て、計算する
        for (int i = 0; i < CalcMeasureLine_Calc.Count; i++)
        {
            //各拍に変速オプションが存在するか確認、存在する場合停止・変速に合わせて特殊処理を行う
            for (int j = NowBeat_Calc; NowBeat_Calc < j+CalcMeasureLine_Calc[i]; NowBeat_Calc++)
            {
                //変速検出
                if (OptionData_Calc[NowBeat_Calc].Option[0] == 2)//変速
                {
                    NowBPM_Calc = OptionData_Calc[NowBeat_Calc].Option[1];
                }
                if(OptionData_Calc[NowBeat_Calc].Option[0] == 1)//停止
                {
                    NowBMSCount_Calc += (double)(Multi*OptionData_Calc[NowBeat_Calc].Option[2]) *DeffBPM_Calc/ (double)OptionData_Calc[NowBeat_Calc].Option[1] / NowBPM_Calc;
                }
                MusicParam.BMSArr.Add(NowBMSCount_Calc);
                for (int k = 0; k < 12; k++) {
                    if (OptionData_Calc[NowBeat_Calc].NotesSet[k, 0] == 2)
                    {
                        bufflong = new LongArr();
                        //ロングノーツを発見したとき、StackLongListにスタックする
                        bufflong.Keynum = k;
                        bufflong.CutBeat = OptionData_Calc[NowBeat_Calc].NotesSet[k,3];
                        bufflong.Count = OptionData_Calc[NowBeat_Calc].NotesSet[k,4];
                        bufflong.NowCount = 0;
                        bufflong.StartLineCount = NowBMSCount_Calc;
                        bufflong.EndLineCount = NowBMSCount_Calc;
                        QueueLongList.Add(bufflong);
                    }
                }
                for(int l= 0; l < QueueLongList.Count; l++)
                {
                    QueueLongList[l].EndLineCount += (double)Multi * DeffBPM_Calc / (double)QueueLongList[l].CutBeat / NowBPM_Calc;
                    QueueLongList[l].Count--;
                    if(QueueLongList[l].Count <= 0)
                    {
                        MusicParam.LongNotesList.Add(QueueLongList[l]);
                        QueueLongList.RemoveAt(l);
                        l--;
                    }
                }
                NowBMSCount_Calc += (double)Multi * DeffBPM_Calc / (double)CalcMeasureLine_Calc[i] / NowBPM_Calc;
            }
        }
        NowBMSCount_Calc += 0;
    }

    /// <summary>
    /// ロングノーツを検出、専用のBMSカウントを生成する
    /// </summary>
    void CalcLongBMSCount()
    {
        List<int> CalcMeasureLine_Calc = MusicParam.Line;
        List<Musicnote> OptionData_Calc = MusicParam.NoteList;
        double DeffBPM_Calc = MusicParam.Bpm;
        double NowBPM_Calc = DeffBPM_Calc;
        int NowBeat_Calc = 0;
        double NowBMSCount_Calc = 0;
        //小節ごとにBMSカウントを割り当て、計算する
        for (int i = 0; i < CalcMeasureLine_Calc.Count; i++)
        {
            //各拍に変速オプションが存在するか確認、存在する場合停止・変速に合わせて特殊処理を行う
            for (int j = NowBeat_Calc; NowBeat_Calc < j + CalcMeasureLine_Calc[i]; NowBeat_Calc++)
            {
                //変速検出
                if (OptionData_Calc[NowBeat_Calc].Option[0] == 2)
                {
                    NowBPM_Calc = OptionData_Calc[NowBeat_Calc].Option[1];
                }
                if (OptionData_Calc[NowBeat_Calc].Option[0] == 1)
                {
                    NowBMSCount_Calc += (double)(Multi * OptionData_Calc[NowBeat_Calc].Option[2]) * DeffBPM_Calc / (double)OptionData_Calc[NowBeat_Calc].Option[1] / NowBPM_Calc;
                }
                MusicParam.BMSArr.Add(NowBMSCount_Calc);
                NowBMSCount_Calc += (double)Multi * DeffBPM_Calc / (double)CalcMeasureLine_Calc[i] / NowBPM_Calc;
            }
        }
    }
    protected IEnumerator OffsetWaiter(float offset)
    {
        Debug.Log("StartWaiting!");
        yield return new WaitForSeconds((float)MusicParam.Offset/1000);
    }
    /*void Start()
    {
        temp = MusicParam.Bpm;
        MusicParam.DeffBPM = MusicParam.Bpm;
        MusicParam.Bpm = 0;
        try
        {
            while (BMSCount + Multi * 4 > MusicParam.BMSArr[NowBeat])
            {
                for (int j = 0; j < 12; j++)
                {
                    if (MusicParam.NoteList[NowBeat].NotesSet[j, 0] == 1)
                    {
                        GameObject.Find("NotesGenerator").GetComponent<NotesGenerator>().NotesGenarate(j, MusicParam.NoteList[NowBeat].NotesSet[j, 1], MusicParam.BMSArr[NowBeat]);
                    }
                    else if (MusicParam.NoteList[NowBeat].NotesSet[j, 0] == 2)
                    {
                        GameObject.Find("NotesGenerator").GetComponent<NotesGenerator>().LongGenerate(j, MusicParam.NoteList[NowBeat].NotesSet[j, 1], MusicParam.NoteList[NowBeat].NotesSet[j, 2], MusicParam.LongNotesList[LongCheckCount].StartLineCount, MusicParam.LongNotesList[LongCheckCount].EndLineCount);
                        LongCheckCount++;
                    }else if (MusicParam.NoteList[NowBeat].NotesSet[j, 0] == 3)
                    {
                        GameObject.Find("NotesGenerator").GetComponent<NotesGenerator>().FlickGenerate(j, MusicParam.NoteList[NowBeat].NotesSet[j, 1], MusicParam.NoteList[NowBeat].NotesSet[j, 2], MusicParam.BMSArr[NowBeat]);
                    }
                }
                NowBeat++;
            }
        }
        catch (ArgumentOutOfRangeException)
        {
            if (!endflag)
            {
                Debug.Log("EndNotes");
            }
            endflag = true;
        }
    }
    // Update is called once per frame
    void Update () {
        FPS = 1f / Time.deltaTime;
        if (Offset > MusicParam.Offset)
        {
            if (OffsetFlag == false)
            {
                OffsetFlag = true;
                MusicParam.Bpm = temp;
            }
            try
            {
                while (BMSCount + Multi * 4 > MusicParam.BMSArr[NowBeat])
                {
                    for (int j = 0; j < 12; j++)
                    {
                        if (MusicParam.NoteList[NowBeat].NotesSet[j, 0] == 1)
                        {
                            GameObject.Find("NotesGenerator").GetComponent<NotesGenerator>().NotesGenarate(j, MusicParam.NoteList[NowBeat].NotesSet[j, 1], MusicParam.BMSArr[NowBeat]);
                        }
                        else if (MusicParam.NoteList[NowBeat].NotesSet[j, 0] == 2)
                        {
                            GameObject.Find("NotesGenerator").GetComponent<NotesGenerator>().LongGenerate(j, MusicParam.NoteList[NowBeat].NotesSet[j, 1], MusicParam.NoteList[NowBeat].NotesSet[j, 2], MusicParam.LongNotesList[LongCheckCount].StartLineCount, MusicParam.LongNotesList[LongCheckCount].EndLineCount);
                            LongCheckCount++;
                        }
                    }
                    NowBeat++;
                }

            }
            catch (ArgumentOutOfRangeException)
            {
                if (!endflag)
                {
                    Debug.Log("EndNotes");
                }
                endflag = true;
            }
            BMSCount += Multi * (MusicParam.DeffBPM/ 60.0 / 4.0) / FPS;  //現在のBMSカウント
        }else
        {
            Offset += Time.deltaTime*1000;
        }
        //BPM = 0
        //遅延処理
        //BPM = default

    }*/
}
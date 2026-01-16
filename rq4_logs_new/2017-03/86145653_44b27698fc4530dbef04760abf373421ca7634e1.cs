using UnityEngine;
using UnityEngine.UI;
using System.Collections;

enum AIState
{
    ANALYZING,
    SLEEPING,
}
public class PuzzleAnalyzer : MonoBehaviour {

    private AIState state;

    [SerializeField]
    private MainGameSystem sys;
    [SerializeField, Range(0, 5.0f)]
    private float ExecSpeed;

    [SerializeField]
    private Button StartAIButton, StopAIButton;

	// Use this for initialization
	void Start () {
        if(sys == null)
            sys = GameObject.Find("GameController").GetComponent<MainGameSystem>();
        if (StartAIButton == null)
            StartAIButton = GameObject.Find("StartAIButton").GetComponent<Button>();
        if(StopAIButton)
            StartAIButton = GameObject.Find("StopAIButton").GetComponent<Button>();
        state = AIState.SLEEPING;
	}

	
	// Update is called once per frame
	void Update () {
	}

    public void StartAI()
    {
        if (this.state == AIState.SLEEPING) {
            print("Activate");
            StartCoroutine(AnalyzePuzzle());
        }
    }

    public void StopAI()
    {
        if (this.state == AIState.ANALYZING) {
            print("Sleep");
            StopCoroutine(AnalyzePuzzle());
        }
    }

    IEnumerator AnalyzePuzzle()
    {
        while (true)
        {
            // 消すパネルが見つかるまで実行
            while (!SolvePuzzle(Random.Range(0, sys.stage.GetLength(0)), Random.Range(0, sys.stage.GetLength(0)))) ;
            yield return new WaitForSeconds(ExecSpeed);
        }
    }

    private bool SolvePuzzle(int i,int j)
    {
        if (sys.stage[i, j] == null) return false;
        // ステージのブロックの色を配列に格納
        BlockType[,] colors = new BlockType[sys.stage.GetLength(0), sys.stage.GetLength(1)];
        for (int x = 0; x < sys.stage.GetLength(0); x++)
        {
            for (int y = 0; y < sys.stage.GetLength(1); y++)
            {
                if (sys.stage[x,y] != null && sys.stage[x,y].GetComponent<Block>().BlockStatus == BlockStatus.NORMAL)
                    colors[x, y] = sys.stage[x, y].GetComponent<Block>().BlockType;
            }
        }
        // 上2マスが同じ色の場合
        if (i - 1 > 0 && colors[i - 1, j] == colors[i - 2, j])
        {
            // 左のマスが上2マスと同じ色の場合
            if (j > 0 && colors[i - 1, j] == colors[i, j - 1]) {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i - 1, j].GetComponent<Block>(), sys.stage[i, j].GetComponent<Block>()));
                return true;
            }
            // 下のマスが上2マスと同じ色
            else if (i + 1 < sys.stage.GetLength(0) && colors[i - 1, j] == colors[i + 1, j])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i - 1, j].GetComponent<Block>(), sys.stage[i,j].GetComponent<Block>()));
                return true;
            }
            // 右のマスが上2マスと同じ色
            else if (j + 1 < sys.stage.GetLength(1) && colors[i - 1, j] == colors[i, j + 1])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i, j + 1].GetComponent<Block>()));
                return true;
            }
        }
        //以下同様に判定を行う
        // 左2マスが同じ色の場合
        if (j - 1 > 0 && colors[i, j - 1] == colors[i, j - 2])
        {
            if (i > 0 && colors[i, j - 1] == colors[i - 1, j])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i - 1, j].GetComponent<Block>()));
                return true;
            }
            else if (i + 1 < sys.stage.GetLength(0) && colors[i, j - 1] == colors[i + 1, j])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i + 1, j].GetComponent<Block>()));
                return true;
            }
            else if (j + 1 < sys.stage.GetLength(1) && colors[i, j - 1] == colors[i, j + 1])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i, j + 1].GetComponent<Block>()));
                return true;
            }
        }
        // 下2マスが同じ色の場合
        if (i + 2 < sys.stage.GetLength(0) && colors[i + 1, j] == colors[i + 2, j])
        {
            if (i > 0 && colors[i + 1, j] == colors[i - 1, j])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i - 1, j].GetComponent<Block>()));
                return true;
            }
            else if (j > 0 && colors[i + 1, j] == colors[i, j - 1])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i, j - 1].GetComponent<Block>()));
                return true;
            }
            else if (j + 1 < sys.stage.GetLength(1) && colors[i + 1, j] == colors[i, j + 1])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i, j + 1].GetComponent<Block>()));
                return true;
            }
        }
        // 右2マスが同じ色の場合
        if (j + 2 < sys.stage.GetLength(0) && colors[i, j + 1] == colors[i, j + 2])
        {
            if (i > 0 && colors[i, j + 1] == colors[i - 1, j])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i - 1, j].GetComponent<Block>()));
                return true;
            }
            else if (j > 0 && colors[i, j + 1] == colors[i, j - 1])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i, j - 1].GetComponent<Block>()));
                return true;
            }
            else if (i + 1 < sys.stage.GetLength(1) && colors[i, j + 1] == colors[i + 1, j])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i + 1, j].GetComponent<Block>()));
                return true;
            }
        }
        // 上下2マスが同じ色の場合
        if ((i > 0 && i + 1 < sys.stage.GetLength(0)) && colors[i - 1, j] == colors[i + 1, j])
        {
            if (j > 0 && colors[i - 1, j] == colors[i, j - 1])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i, j - 1].GetComponent<Block>()));
                return true;
            }
            else if (j + 1 < sys.stage.GetLength(1) && colors[i - 1, j] == colors[i, j + 1])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i, j + 1].GetComponent<Block>()));
                return true;
            }
        }
        // 左右2マスが同じ色の場合
        if ((j > 0 && j + 1 < sys.stage.GetLength(0)) && colors[i, j - 1] == colors[i, j + 1])
        {
            if (i > 0 && colors[i, j - 1] == colors[i - 1, j])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i - 1, j].GetComponent<Block>()));
                return true;
            }
            else if (i + 1 < sys.stage.GetLength(1) && colors[i, j - 1] == colors[i + 1, j])
            {
                sys.StartCoroutine(sys.SwitchBlock(sys.stage[i, j].GetComponent<Block>(), sys.stage[i + 1, j].GetComponent<Block>()));
                return true;
            }
        }
        return false;
    }
}
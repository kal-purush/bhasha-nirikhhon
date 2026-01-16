using UnityEngine;
using UnityEngine.UI;

public class ChainManager : MonoBehaviour {

    // 現在のチェイン数
    private int Chain = 0;
    // 現在の最大チェイン数
    private int MaxChain = 0;
    // Textコンポーネント
    [SerializeField]
    private Text ChainText, MaxChainText;
    // Use this for initialization
    void Start () {
	    
	}
	
	// Update is called once per frame
	void Update () {
	
	}

    // チェイン数加算処理
    public void PlusChainNum() {
        this.CurrentChainProp++;
        this.ChainText.text = "Chain " + this.CurrentChainProp;
        if (this.CurrentChainProp > this.MaxChainProp)
        {
            this.MaxChainProp = this.CurrentChainProp;
            this.MaxChainText.text = "MaxChain " + this.MaxChainProp;
        }
    }

    // チェイン数を1に初期化
    public void ResetChainNum() {
        this.CurrentChainProp = 1;
        this.ChainText.text = "Chain " + this.CurrentChainProp;
    }

    public int CurrentChainProp
    {
        get { return this.Chain; }
        set { this.Chain = value; }
    }
    public int MaxChainProp
    {
        get { return this.MaxChain; }
        set { this.MaxChain = value; }
    }
}
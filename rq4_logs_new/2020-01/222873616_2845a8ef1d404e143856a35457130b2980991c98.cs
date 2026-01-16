using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using TMPro;

namespace HungraviyEx2019 {

    /// <summary>
    /// 表示するオブジェクトの指示構造体
    /// </summary>
    [System.Serializable]
    public struct DisplayObject
    {
        /// <summary>
        /// 対応するインデックス
        /// </summary>
        public int index;
        /// <summary>
        /// 表示するゲームオブジェクト
        /// </summary>
        public GameObject gameObject;

        public DisplayObject(int idx, GameObject go)
        {
            index = idx;
            gameObject = go;
        }
    }

    public class Tutorial : MonoBehaviour
    {
        public static Tutorial instance = null;

        [Tooltip("最低表示秒数"), SerializeField]
        float displayTimeMin = 2f;
        [Tooltip("メッセージ表示待ち秒数"), SerializeField]
        float nextTime = 4f;
        [Tooltip("表示オブジェクト"), SerializeField]
        DisplayObject[] displayObjects;

        static readonly string[] tutorialTexts =
        {
            // 0
            "画面をクリック(タップ)して、<color=#faa>ブラックホール</color>を発生させます。",
            // 1 押す
            "<color=#faa>エネルギー</color>がある間、ブラックホールを\n出し続けられます。",
            // 2 離す
            "エネルギーは、ブラックホールを出さなければ\n回復します。",
            // 3 時間
            "主人公の<color=#faa>ぐらびぃ</color>にブラックホールを\n近づけると、吸い寄せられて移動します。",
            // 4 時間
            "ブラックホールが<color=#faa>近い</color>ほど、強く吸い寄せます。",
            // 5 エミッター
            "ブラックホールで<color=#faa>アイテム</color>を吸い寄せて、\n<color=#faa>ぐらびぃ</color>まで運ぶと食べます。",
            // 6 時間
            "アイテムを食べると、エネルギーが回復します。",
            // 7 エミッター
            "<color=#faa>敵</color>(　　)をブラックホールで吸い込めば\nやっつけて<color=#faa>アイテム</color>にできます。",
            // 8 エミッター
            "<color=#faa>敵</color>や<color=#faa>トゲ</color>にぶつかると<color=#faa>LIFE</color>が減ります。",
            // 9 時間
            "<color=#faa>LIFE</color>がなくなると<color=#faa>ゲームオーバー</color>です。",
            // 10 時間
            "<color=#faa>LIFE</color>は、<color=#faa>ハート</color>を食べると回復します。",
            // 11 エミッター
            "この<color=#faa>敵</color>(　　)はやっつけられないので、\nよけてください。",
            // 12 時間
            "では、ゴールの<color=#faa>スイーツ</color>目指して、\n<color=#faa>冒険</color>をはじめましょう！",
            // 13 時間
            ""
        };

        static TextMeshProUGUI tutorialTextObject = null;
        static int index = -1;

        /// <summary>
        /// メッセージの表示を開始してからの経過秒数
        /// </summary>
        static float displayTime = 0;

        /// <summary>
        /// 到達させたいインデックス
        /// </summary>
        public static int requestIndex = -1;

        private void Awake()
        {
            instance = this;
            tutorialTextObject = GetComponent<TextMeshProUGUI>();
            tutorialTextObject.text = "";
        }

        void Start()
        {
            index = -1;
            requestIndex = -1;
        }

        void FixedUpdate()
        {
            if (index == -1) return;

            checkUpdateIndex();

            displayTime += Time.fixedDeltaTime;
            if (requestIndex <= index)
            {
                return;
            }
            // ページを更新
            if (displayTime >= displayTimeMin)
            {
                Next();
            }
        }

        void checkUpdateIndex()
        {
            switch(index)
            {
                case 0:
                    if (Blackhole.IsSpawn)
                    {
                        requestIndex = 1;
                    }
                    break;

                case 1:
                    if (!Blackhole.IsSpawn)
                    {
                        requestIndex = 2;
                    }
                    break;

                case 2:
                case 5:
                case 8:
                case 9:
                case 11:
                case 12:
                    if (displayTime >= nextTime)
                    {
                        requestIndex = index + 1;
                    }
                    break;
            }
        }

        /// <summary>
        /// 次のチュートリアルを表示します。最初に開始する時も、これを呼びます。
        /// </summary>
        /// <returns>表示できたらtrueを返します。次のページがなければfalse</returns>
        public static bool Next()
        {
            if (instance == null) return false;

            // 前に表示していたオブジェクトを消す
            instance.SetObjectActive(index, false);

            index++;
            if (index >= tutorialTexts.Length)
            {
                return false;
            }

            displayTime = 0f;
            tutorialTextObject.text = tutorialTexts[index];
            instance.SetObjectActive(index, true);

            return true;
        }

        /// <summary>
        /// 指定のインデックスのオブジェクトがあれば、flagで指定した表示状態にします。
        /// </summary>
        /// <param name="index">指定のインデックス</param>
        /// <param name="flag">表示状態</param>
        void SetObjectActive(int index, bool flag)
        {
            for (int i = 0; i < displayObjects.Length; i++)
            {
                if (displayObjects[i].index == index)
                {
                    displayObjects[i].gameObject.SetActive(flag);
                    break;
                }
            }
        }
    }
}
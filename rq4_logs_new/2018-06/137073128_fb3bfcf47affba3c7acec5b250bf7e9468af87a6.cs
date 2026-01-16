using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;

public class CubeTower23 : MonoBehaviour {

    public Material mat;
    Transform cam;
    int cnt; // 段数
    float total_area; // 接地面積合計
    public Text text;

    Transform preCube; // 前に積み上げたキューブ
    Transform prePreCube; // 前の前に積み上げたCube

    GameObject cubeParent;
    Vector3[] cubes_pre_calc_rigidBody; // RigidBody付加前の位置座標を保存する。
    Quaternion[] cubes_rot_pre_calc_rigidBody;

    Vector3 cubeSize;

    Vector3 initCamPos;

    const float move = 0.05f;
    bool evaluateMode;

    public Transform ground;

    float countStartTime;

    float score;

    bool isStartGame;

    public Canvas canvas;
    public GameObject gameStartText;

    float evaluateMode_view_length;

    string tokuten_keisan;

    float CalcTokuten()
    {
        float h = preCube.position.y;
        if (h < 0)
        {
            return 0;
        }

        float ave_area = cubeSize.x * cubeSize.z * cnt;
        float area_rate = (total_area / ave_area);
        return h / area_rate;
    }

    void Start () {
        Application.targetFrameRate = 60;
        cam = Camera.main.transform;
        cubeParent = new GameObject();
        cubeParent.name = "cubeParent";
        cubeSize = new Vector3(4, 1f, 4);
        ground.position = -Vector3.up * cubeSize.y;
        initCamPos = cam.position;
        isStartGame = false;
        tokuten_keisan = " 高さ/(面積累積/真上に積み重ねた場合の面積累積) ";

        Init();
    }

    void Init()
    {
        cnt = 0;
        preCube = null;
        prePreCube = null;
        total_area = 0;
        evaluateMode = false;
        countStartTime = -1;
        GenCube();
    }

    // Cubeを生成する。
    void GenCube()
    {
        var go = GameObject.CreatePrimitive(PrimitiveType.Cube);
        go.transform.parent = cubeParent.transform;
        go.transform.localScale = cubeSize;

        // 前のキューブがあれば、クリックしたとき、
        if (preCube != null)
        {
            // 前のキューブに色を付ける。
            float r = Random.Range(0, 1f);
            float g = Random.Range(0, 1f);
            float b = Random.Range(0, 1f);
            var mm = new Material(Shader.Find("Diffuse"));
            mm.color = new Color(r, g, b);
            preCube.GetComponent<MeshRenderer>().material = mm;

            // 新しいキューブの位置を一段上げる。
            go.transform.position = preCube.position + Vector3.up * cubeSize.y * 1.01f; // 少し隙間を空ける。

            // 前の前のキューブがあれば、
            if (prePreCube != null)
            {
                // 設置面積を求める。
                Vector3 diff = preCube.position - prePreCube.position; // 前のキューブと前の前のキューブの中心位置がどれぐらいずれているか。

                float xx = preCube.localScale.x / 2 + prePreCube.localScale.x / 2; // x軸での接地幅を求める。
                float dx = xx - Mathf.Abs(diff.x);
                dx = Mathf.Max(0, dx);

                float zz = preCube.localScale.z / 2 + prePreCube.localScale.z / 2;
                float dz = zz - Mathf.Abs(diff.z);
                dz = Mathf.Max(0, dz);

                total_area += dx * dz;
                // print("設置面積合計 : " + total_area);
                float h = preCube.position.y;
                text.text = "接地面積合計 : " + total_area.ToString() + System.Environment.NewLine + "最上段Cube高さ : " + h.ToString() + System.Environment.NewLine + "得点[" + tokuten_keisan + "] : " + CalcTokuten().ToString();
            }
            prePreCube = preCube; // 前の前のCubeを前のキューブにする。
        }

        go.GetComponent<MeshRenderer>().material = mat;

        // cam.parent = preCube; // カメラ移動のために前のキューブを親にする
        preCube = go.transform; // 新しく生成したキューブを前のキューブに設定する。

        cnt++; // 一段上げる。
    }


    // Update is called once per frame
    void Update()
    {
        float _time = Time.frameCount * 2 * Mathf.PI / (60 * 60); // カメラ回転速度、60秒で一周する。

        if (!isStartGame)
        {
            float r = 5; // カメラ回転半径
            float h = initCamPos.y;
            cam.localPosition = new Vector3(r * Mathf.Cos(_time), h, r * Mathf.Sin(_time)); // 常に4上にカメラが位置するようにする。
            cam.LookAt(preCube);

            if (Time.frameCount % 45 == 0)
            {
                gameStartText.SetActive(!gameStartText.activeSelf);
            }

            if (Input.GetKeyDown(KeyCode.Return))
            {
                isStartGame = true;

                for (int i = 0; i < canvas.transform.childCount; i++)
                {
                    var g = canvas.transform.GetChild(i).gameObject;
                    g.SetActive(!g.activeSelf);
                }

                //var as_ = new GameObject().AddComponent<AudioSource>();
                //as_.clip = start_call;
                //as_.Play();

                gameStartText.SetActive(false);
            }
            
            return;
        }

        if (Input.GetKeyDown(KeyCode.R))
        {
            ResetGame();
        }

        if (!evaluateMode)
        {
            if (Input.GetKeyDown(KeyCode.Space))
            {
                GenCube();
            }

            // カメラから見た方向にCubeが動くようにする。
            Vector3 dir = (preCube.position - cam.position).normalized;
            dir.y = 0;
            Vector3 v = dir * move;
            // カメラから見た方向をローカルz軸としたとき、x軸を求めたい。これにはy軸との外積を使う。
            Vector3 v_yoko = Vector3.Cross(Vector3.up, dir) * move;

            if (Input.GetKey(KeyCode.W))
            {
                preCube.position += v;
            }
            else if (Input.GetKey(KeyCode.S))
            {
                preCube.position -= v;
            }

            if (Input.GetKey(KeyCode.D))
            {
                preCube.position += v_yoko;
            }
            else if (Input.GetKey(KeyCode.A))
            {
                preCube.position -= v_yoko;
            }

            // カメラ回転処理
            float r = 10; // カメラ回転半径
            float h = initCamPos.y;
            if (preCube != null)
            {
                h += preCube.position.y;
            }
            cam.localPosition = new Vector3(r * Mathf.Cos(_time), h, r * Mathf.Sin(_time)); // 常に4上にカメラが位置するようにする。
            cam.LookAt(preCube);
        }
        else
        {
            Vector3 towerCenter = preCube.position / 2; // y以外は0で、2で割れば、高さの半分が求まるとする。
            float r = evaluateMode_view_length;
            cam.localPosition = new Vector3(r * Mathf.Cos(_time), 4, r * Mathf.Sin(_time)); // ここは、すごく高くなると4だと見えなくなりそうだけど、まあいいや。
            cam.LookAt(towerCenter);

            if (countStartTime > 0)
            {
                float h = preCube.position.y;
                float tt = 5 - (Time.time - countStartTime);
                score = CalcTokuten(); ;
                text.text = tt.ToString() + " 秒後のスコアが採用されます." + System.Environment.NewLine + "高さ : " + h.ToString() + System.Environment.NewLine + "スコア : " + score.ToString();
                if (tt < 0)
                {
                    string point = "スコア[" + tokuten_keisan + "] : " + score.ToString();
                    text.text = point;
                    countStartTime = -1;
                }
            }
        }

        if (Input.GetKeyDown(KeyCode.E))
        {
            ChangeView();
        }

        if (Input.GetKeyDown(KeyCode.C))
        {
            ClacRigidBody();
        }
    }

    public void ClacRigidBody()
    {
        if (!evaluateMode)
        {
            text.text = "視点を変えてください.";
            return;
        }

        cubes_pre_calc_rigidBody = new Vector3[cubeParent.transform.childCount];
        cubes_rot_pre_calc_rigidBody = new Quaternion[cubeParent.transform.childCount];
        for (int i = 0; i < cubeParent.transform.childCount; i++)
        {
            Transform t = cubeParent.transform.GetChild(i);
            cubes_pre_calc_rigidBody[i] = t.position;
            cubes_rot_pre_calc_rigidBody[i] = t.rotation;
            t.gameObject.AddComponent<Rigidbody>();
        }
        countStartTime = Time.time;
    }

    public void ChangeView()
    {
        if (total_area > 0)
        {
            evaluateMode = !evaluateMode;

            float fov = cam.GetComponent<Camera>().fieldOfView;
            evaluateMode_view_length = preCube.position.y / (2 * Mathf.Tan(Mathf.Deg2Rad * fov * 0.5f)) * 1.5f;

            if (evaluateMode)
            {
                cam.parent = null;
            }
            else
            {
                // cam.parent = preCube;
                for (int i = 0; i < cubeParent.transform.childCount; i++)
                {
                    Transform t = cubeParent.transform.GetChild(i);
                    var rb = t.GetComponent<Rigidbody>();
                    if (rb != null)
                    {
                        Destroy(rb);
                    }

                    if (cubes_pre_calc_rigidBody != null) 
                    {
                        if (cubes_pre_calc_rigidBody.Length == cubeParent.transform.childCount)
                        {
                            t.position = cubes_pre_calc_rigidBody[i];
                            t.rotation = cubes_rot_pre_calc_rigidBody[i];
                        }
                    }
                    
                    countStartTime = -1;
                }
            }
        }
        else
        {
            text.text = "面積 0 の状態はダメです." + System.Environment.NewLine + "崩れないようにちゃんと積み上げて！";
        }
    }

    public void ResetGame()
    {
        cam.parent = null;
        for (int i = 0; i < cubeParent.transform.childCount; i++)
        {
            Destroy(cubeParent.transform.GetChild(i).gameObject);
        }

        Init();
    }
}
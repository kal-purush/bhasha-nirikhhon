using System.IO;
using UnityEngine;
using TeamB_TD;
using TeamB_TD.SaveData;


namespace TeamB_TD
{
    namespace SaveData
    {
        public class DataManager : MonoBehaviour
        {
            [HideInInspector] public SaveData _pData;
            string _filepath;
            public static string _fileName = "SaveData.json";
            public static string _json;

            public SaveData PlayerData => _pData;

            public static string GetPDataFileName()
            {
                return _fileName;
            }
            public static string GetPDataJson()
            {
                return _json;
            }
            private void Awake()
            {
                _filepath = Application.dataPath + "/SaveData/" + _fileName;

                if (!File.Exists(_filepath))
                {
                    Debug.Log(_filepath);
                    Save(_pData);
                }
                _pData = Load(_filepath);
            }
            void Save(SaveData data)
            {
                _json = JsonUtility.ToJson(data);
                StreamWriter wr = new StreamWriter(_filepath, false);
                wr.WriteLine(_json);
                wr.Close();
            }
            SaveData Load(string path)
            {
                StreamReader rd = new StreamReader(path);
                _json = rd.ReadToEnd();
                rd.Close();

                return JsonUtility.FromJson<SaveData>(_json);
            }
            private void OnDestroy()
            {
                //Save(_pData);
            }
            // 以下デバッグ用
            /*
            public void OverWrite(int stagenum) //特定のステージのクリア状況を変更する
            {
                StreamReader rd = new StreamReader(_filepath);
                _json = rd.ReadToEnd();
                rd.Close();
                SaveData memory = JsonUtility.FromJson<SaveData>(_json);
                for (int i = 0; i < stagenum; i++)
                {
                    memory._isClear[i] = true;
                }
                _json = JsonUtility.ToJson(memory);
                StreamWriter wr = new StreamWriter(_filepath, false);
                wr.WriteLine(_json);
                wr.Close();
            }
            public void OverWriteAll()
            {
                StreamReader rd = new StreamReader(_filepath);
                _json = rd.ReadToEnd();
                rd.Close();
                SaveData memory = JsonUtility.FromJson<SaveData>(_json);
                for (int i = 0; i < memory._isClear.Length; i++)
                {
                    memory._isClear[i] = true;
                }
                _json = JsonUtility.ToJson(memory);
                StreamWriter wr = new StreamWriter(_filepath, false);
                Debug.Log(_json);
                wr.WriteLine(_json);
                wr.Close();
            }
            
            public void Clearzero()
            {
                int num = 0;
                OverWrite(num);
            }
            public void Clear1st()
            {
                int num = 1;
                OverWrite(num);
            }
            public void Clear2nd()
            {
                int num = 2;
                OverWrite(num);
            }
            public void Clear3rd()
            {
                int num = 3;
                OverWrite(num);
            }
            public void Reset()
            {
                StreamReader rd = new StreamReader(_filepath);
                _json = rd.ReadToEnd();
                rd.Close();
                SaveData memory = JsonUtility.FromJson<SaveData>(_json);
                for (int i = 0; i < memory._isClear.Length; i++)
                {
                    memory._isClear[i] = false;
                }
                _json = JsonUtility.ToJson(memory);
                StreamWriter wr = new StreamWriter(_filepath, false);
                Debug.Log(_json);
                wr.WriteLine(_json);
                wr.Close();
            }
            */

        }
    }
}

    





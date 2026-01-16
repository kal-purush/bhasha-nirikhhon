using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using Glib.InspectorExtension;

namespace TeamB_TD
{
    namespace UI
    {
        public class SceneTransition : MonoBehaviour
        {
            [Header("次のシーンに移るまでの時間")]
            [SerializeField] private float _delayTime = 0f;
            [Header("移り先のシーンの名前")]
            [SerializeField, SceneName] private string _sceneName = default;

            public static SceneTransition instance;
            public void Awake()
            {
                if (instance == null)
                {
                    instance = this;
                    DontDestroyOnLoad(this.gameObject);
                }
                else
                {
                    Destroy(this.gameObject);
                }

            }
            //public void SceneTrans()
            //{
            //    SceneManager.LoadScene(_sceneName);
            //}

            public IEnumerator SceneTrans()
            {
                yield return new WaitForSeconds(_delayTime);
                SceneManager.LoadScene(_sceneName);
            }
        }




    }


}


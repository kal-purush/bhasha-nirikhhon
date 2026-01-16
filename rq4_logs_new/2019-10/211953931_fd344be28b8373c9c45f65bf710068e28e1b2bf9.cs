using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.SceneManagement;
using UnityEngine.UI;

namespace Jennings 
{
    public class ChangeScene : MonoBehaviour
{
        private static ChangeScene current;

        void Start()
        {

            if (current == null)
            {
                current = this;
                DontDestroyOnLoad(gameObject);
            }
            else
            {
                Destroy(gameObject);
            }

        }

        public void LoadNextScene()
        {
            void OnPointerDown()
            {
                //print("CLICKED");
                SceneManager.LoadScene("JenningsScene02", LoadSceneMode.Single);
            }
        }
    }
}
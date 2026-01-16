using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using UnityEngine.UI;
using UnityEngine.SceneManagement;

namespace Breu
{
    public class BreuHudController : MonoBehaviour
    {

        private static BreuHudController current;
        // Start is called before the first frame update
        void Start()
        {
            if (current == null)
            {
                current = this;
                DontDestroyOnLoad(gameObject);
            }
            else
            {
                Destroy(this);
            }
        }

        // Update is called once per frame
        void Update()
        {

        }



        #region Load Levels
        public void loadLevel2()
        {
            BreuInventory.main.tutorialKey = true;
            SceneManager.LoadScene("BreuScene02", LoadSceneMode.Single);
            print("button");
        }

        #endregion
    }
}
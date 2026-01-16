using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace InterDineMension.Appartment
{
    public class AptChrUp : AptUpParent
    {
        public string pPrefChrKey, pPrefAffectionKey, pPrefChrChaosKey;
        public int pPrefChrConvoNum, chaosUnlockNum;
        public GameObject goodEnd, badEnd, chaosEnd;
        //public TextAsset goodEnddesc, badEnddesc, chaosEnddesc;


        // Start is called before the first frame update
        void Start()
        {
            if (PlayerPrefs.GetInt(pPrefChrKey) >= pPrefChrConvoNum)
            {
                if (PlayerPrefs.GetInt(pPrefChrChaosKey) >= chaosUnlockNum)
                {
                    chaosEnd.SetActive(true);
                }
                else
                {
                    if (PlayerPrefs.GetInt(pPrefAffectionKey) >= 0)
                    {
                        goodEnd.SetActive(true);
                    }
                    else
                    {
                        badEnd.SetActive(true);
                    }
                }

            }
            else
            {
                gameObject.SetActive(false);
            }

            // Update is called once per frame
            /*void Update()
            {

            }*/
        }
    }
}
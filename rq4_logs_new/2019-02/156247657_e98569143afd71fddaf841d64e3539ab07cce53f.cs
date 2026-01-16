using System.Collections;
using System.Collections.Generic;
using UnityEngine;

namespace MastersOfTempest
{
    public class TutorialBookHandler : MonoBehaviour
{
 
        public Spellbook spellbook;
        public GameObject book;


        void Start()
        {
            spellbook.OpenOrClose();
        }

        void Update()
    {
        if (Input.GetKeyDown(KeyCode.F))
        {
            spellbook.OpenOrClose();
        }

        if (Input.GetKeyDown(KeyCode.E))
        {
            spellbook.NextPage();
        }

        if (Input.GetKeyDown(KeyCode.Q))
        {
            spellbook.PreviousPage();
        }
    }
    }

}
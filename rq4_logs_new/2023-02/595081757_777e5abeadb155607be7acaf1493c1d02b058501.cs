using System;
using System.Collections;
using System.Collections.Generic;
using Audio;
using UnityEditor;
using UnityEngine;

namespace Puzzle
{
    public class CabinetPuzzle : MonoBehaviour
    {
        [SerializeField]
        private List<GameObject> state1Objs, state2Objs, state3Objs, state4Objs, state5Objs, state6Objs;

        [SerializeField] private List<DialogueSo> state1AutoDialogue,
            state2AutoDialogue,
            state3AutoDialogue,
            state4AutoDialogue,
            state5AutoDialogue,
            state6AutoDialogue;

        private List<List<GameObject>> _stateLists = new List<List<GameObject>>();

        public enum CabinetState
        {
            Outside,
            Inside,
            HasSpokenAtCabinet,
            HasDoneBoxQte,
            HasPushedSofa,
            HasOpenedCabinet
        }

        public CabinetState puzzleState = CabinetState.Outside;

        private void Awake()
        {
            _stateLists.Add(state1Objs);
            _stateLists.Add(state2Objs);
            _stateLists.Add(state3Objs);
            _stateLists.Add(state4Objs);
            _stateLists.Add(state5Objs);
            _stateLists.Add(state6Objs);

            DisableAllObjs();
        }

        private void Update()
        {
            if (Input.GetKeyDown(KeyCode.U))
            {
                UpdatePuzzleState(puzzleState);
            }
        }

        public void UpdatePuzzleState(CabinetState newState)
        {
            puzzleState = newState;

            DisableAllObjs();

            switch (puzzleState)
            {
                //state 1
                case CabinetState.Outside:

                    StartStateDialogue(state1AutoDialogue);

                    foreach (GameObject obj in state1Objs)
                    {
                        obj.SetActive(true);
                    }

                    break;
                //state 2
                case CabinetState.Inside:
                    
                    StartStateDialogue(state2AutoDialogue);
                    
                    foreach (GameObject obj in state2Objs)
                    {
                        obj.SetActive(true);
                    }

                    break;
                //state 3
                case CabinetState.HasSpokenAtCabinet:

                    StartStateDialogue(state3AutoDialogue);
                    
                    foreach (GameObject obj in state3Objs)
                    {
                        obj.SetActive(true);
                    }

                    break;
                //state 4
                case CabinetState.HasDoneBoxQte:

                    StartStateDialogue(state4AutoDialogue);
                    
                    foreach (GameObject obj in state4Objs)
                    {
                        obj.SetActive(true);
                    }

                    break;
                //state 5
                case CabinetState.HasPushedSofa:

                    StartStateDialogue(state5AutoDialogue);
                    
                    foreach (GameObject obj in state5Objs)
                    {
                        obj.SetActive(true);
                    }

                    break;
                //state 6
                case CabinetState.HasOpenedCabinet:

                    StartStateDialogue(state6AutoDialogue);
                    
                    foreach (GameObject obj in state6Objs)
                    {
                        obj.SetActive(true);
                    }

                    break;
            }
        }

        private void StartStateDialogue(List<DialogueSo> diaList)
        {
            foreach (DialogueSo dia in diaList)
            {
                DialogueCanvas.Instance.QueueDialogue(dia);
            }
        }
        
        /*private IEnumerator DelayFunction(float delayTime, System.Action method)
        {
            yield return new WaitForSeconds(delayTime);

            method();
        }*/

        private void DisableAllObjs()
        {
            foreach (List<GameObject> objList in _stateLists)
            {
                foreach (GameObject obj in objList)
                {
                    obj.SetActive(false);
                }
            }
        }
    }
}
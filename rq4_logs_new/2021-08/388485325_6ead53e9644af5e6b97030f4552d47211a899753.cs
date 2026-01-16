using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class FocusTest : MonoBehaviour
{
    public enum FocusState
    {
        Default,
        Process,
        Finish,
    };

    //public int counter = 0;
    public Player player;
    public FocusState fState = FocusState.Default;
    private int fsHash;
    //private int ffHash;
    public Animator playerAN;
    //public int healtphase = 33;


    // Start is called before the first frame update
    void Start()
    {
        player = GetComponent<Player>();
        playerAN = GetComponent<Animator>();
        fsHash = Animator.StringToHash("FocusStart");
        //ffHash = Animator.StringToHash("FocusFinish");
    }

    // Update is called once per frame
    void Update()
    {
        switch (fState)
        {
            case FocusState.Default:
                if (player.playerController.inputFocus && player.soul > 0)
                {
                    playerAN.SetTrigger(fsHash);
                    fState = FocusState.Process;
                }
                break;
            case FocusState.Process:
                fState = FocusState.Finish;
                break;
            case FocusState.Finish:
                //playerAN.SetTrigger(ffHash);
                fState = FocusState.Default;
                break;
        }
    }
}
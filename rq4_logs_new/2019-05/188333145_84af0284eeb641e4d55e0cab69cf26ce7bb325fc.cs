using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class PlayerController : MonoBehaviour
{
    public void MoveUp()
    {
        PlayerMovementCommands.GetMoveUp().Execute(this);
    }
    public void MoveDown()
    {
        PlayerMovementCommands.GetMoveDown().Execute(this);
    }
    public void MoveLeft()
    {
        PlayerMovementCommands.GetMoveLeft().Execute(this);
    }
    public void MoveRight()
    {
        PlayerMovementCommands.GetMoveRight().Execute(this);
    }
    public void MoveUpRight()
    {
        PlayerMovementCommands.GetMoveUpRight().Execute(this);
    }
    public void MoveUpLeft()
    {
        PlayerMovementCommands.GetMoveUpLeft().Execute(this);
    }
    public void MoveDownRight()
    {
        PlayerMovementCommands.GetMoveDownRight().Execute(this);
    }
    public void MoveDownLeft()
    {
        PlayerMovementCommands.GetMoveDownLeft().Execute(this);
    }


    // Start is called before the first frame update
    void Start()
    {
    }

    // Update is called once per frame
    void Update()
    {
        // Should input be handled from here?
    }


}
using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public abstract class AbstractCommand 
{
    public abstract void Execute(GameObject aCharacter, CharacterController aController);


}

public class Jump : AbstractCommand
{

    public override void Execute(GameObject aCharacter, CharacterController aController)
    {
        float jumpHeight = 150.0f;
        float gravity = 2.0f;
        // Vector3 movingDirection = Vector3.zero;
        Vector3 movingDirection = new Vector3(0, 0, 0);

        movingDirection.y = jumpHeight;
        movingDirection.y -= gravity * Time.deltaTime;
        aController.Move(movingDirection * Time.deltaTime);


    }

}
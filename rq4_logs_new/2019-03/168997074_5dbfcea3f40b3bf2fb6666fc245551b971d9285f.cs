using System.Collections;
using System.Collections.Generic;
using UnityEngine;

public class Interact : Instruction
{
    #region Variables
    Interactable targetObject;
    #endregion

    #region Methods
    public Interact(Interactable interactObject, Entity entity) : base(entity)
    {
        targetObject = interactObject;
    }

    public override void Execute()
    {
        // Interact with the object, and fetch the next instruction from it:
        // If this returns null, we are done with this set of instructions.
        Instruction nextInstruction = targetObject.EntityInteract(instructionRunner);
        instructionRunner.instructionEvent.Invoke(nextInstruction);
    }
    #endregion

    #region Functions
    #endregion
}
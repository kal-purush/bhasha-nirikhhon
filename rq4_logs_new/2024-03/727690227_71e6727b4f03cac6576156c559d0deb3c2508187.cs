using OALProgramControl;
using System;
using System.Collections.Generic;
using System.Linq;
using AnimArch.Extensions;

namespace Assets.Scripts.AnimationControl.BuiltIn
{
    public class BuiltInMethodArrayIndexOf : BuiltInMethodArray, IReturnCollector
    {
        private EXEValueBase ReturnedValue; // Only used if .Equals method is applied
        private int CurrentlyEvaluatedIndex;

        public BuiltInMethodArrayIndexOf()
        {
            this.ReturnedValue = null;
            this.CurrentlyEvaluatedIndex = -1;
        }

        public void CollectReturn(EXEValueBase returnedValue)
        {
            this.ReturnedValue = returnedValue;
        }

        protected override EXEExecutionResult Evaluate(EXEValueArray owningObject, List<EXEValueBase> parameters)
        {
            // '.Equals' has been invoked, let us collect the output
            if (this.ReturnedValue != null)
            {
                if (this.ReturnedValue is not EXEValueBool)
                {
                    return EXEExecutionResult.Error(string.Format("Equals method of '{0}' returned '{1}' (it needs to return a bool)", owningObject.TypeName, this.ReturnedValue.TypeName), "XEC2052");
                }

                if ((this.ReturnedValue as EXEValueBool).Value)
                {
                    EXEExecutionResult resultB = EXEExecutionResult.Success();
                    resultB.ReturnedOutput = new EXEValueInt(this.CurrentlyEvaluatedIndex);
                    return resultB;
                }
            }

            CurrentlyEvaluatedIndex++;

            if (CurrentlyEvaluatedIndex >= owningObject.Elements.Count)
            {
                EXEExecutionResult resultB = EXEExecutionResult.Success();
                resultB.ReturnedOutput = new EXEValueInt(-1);
                return resultB;
            }

            EXEValueBase searchedElement = parameters[0];
            EXEExecutionResult result = EXEExecutionResult.Success();

            // Invoke '.Equals' method.
            if
            (
                searchedElement is EXEValueReference
                && (searchedElement as EXEValueReference).TypeClass.MethodExists("Equals", true)
            )
            {
                EXEScopeMethod methodCode = (searchedElement as EXEValueReference).TypeClass.GetMethodByName("Equals", true).ExecutableCode;

                EXEVariable selfVar = new EXEVariable("self", owningObject.Elements[CurrentlyEvaluatedIndex]);
                EXEExecutionResult selfVarAddSuccess = methodCode.AddVariable(selfVar);
                if (!selfVarAddSuccess.IsSuccess)
                {
                    return selfVarAddSuccess;
                }

                EXEVariable param = new EXEVariable("obj", searchedElement);
                EXEExecutionResult paramVarAddSuccess = methodCode.AddParameterVariable(param);
                if (!paramVarAddSuccess.IsSuccess)
                {
                    return paramVarAddSuccess;
                }

                methodCode.MethodCallOrigin = this;
                result.PendingCommand = methodCode;
            }
            // Apply '==' operator.
            else
            {
                int index = -1;
                for (int i = 0; i < owningObject.Elements.Count; i++)
                {
                    if ((searchedElement.IsEqualTo(owningObject.Elements[i]).ReturnedOutput as EXEValueBool).Value)
                    {
                        index = i;
                        break;
                    }
                }

                result.ReturnedOutput = new EXEValueInt(index);
            }

            return result;
        }
    }
}
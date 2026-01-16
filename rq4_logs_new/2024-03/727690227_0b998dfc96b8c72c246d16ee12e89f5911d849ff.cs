using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using UnityEngine;

namespace OALProgramControl
{
    public class MethodInvocatorInfo
    {
        public CDMethod CallerMethod { get; }
        public CDClass CallerClass { get; }
        public CDClassInstance CallerObject { get; }

        public MethodInvocatorInfo(CDMethod callerMethod, CDClass callerClass, CDClassInstance callerObject)
        {
            if (callerMethod == null)
            {
                throw new ArgumentNullException("callerMethod");
            }

            if (callerClass == null)
            {
                throw new ArgumentNullException("callerClass");
            }

            if (callerObject == null)
            {
                throw new ArgumentNullException("callerObject");
            }

            this.CallerMethod = callerMethod;
            this.CallerClass = callerClass;
            this.CallerObject = callerObject;
        }

        public override string ToString()
        {
            return string.Join
            (
                " ",
                new string[]
                {
                    this.CallerMethod?.OwningClass?.Name ?? "NULL",
                    this.CallerClass?.Name ?? "NULL",
                    this.CallerMethod?.Name ?? "NULL",
                }
            );
        }
    }
}
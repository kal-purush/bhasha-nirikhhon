using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using UnityEngine;

namespace Compute.Nodes.Builders
{
    class FunctionNodeBuilder<T> where T : FunctionNode
    {
        T node;
        GameObject thisObject;

        public FunctionNodeBuilder()
        {

        }

        public FunctionNodeBuilder<T> AddExecuteInNode()
        {

            return this;
        }

        public FunctionNodeBuilder<T> AddExecuteOutNode()
        {

            return this;
        }
    }
}
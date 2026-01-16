// ----------------------------------------------------------------------
// <copyright file="TransactionalStateTest.cs" company="SoloX Software">
// Copyright (c) SoloX Software. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Text;
using SoloX.ActionDispatch.Core.UTest.State.Basic;
using Xunit;

namespace SoloX.ActionDispatch.Core.UTest.State
{
    public class TransactionalStateTest
    {
        [Fact]
        public void StateTransactionTest()
        {
            var value = "Test";
            var state = new StateA();
            state.Value = value;
            state.Lock();

            IStateA patchedState;
            using (var transactionalState = state.CreateTransactionalState<IStateA>(state))
            {
                var newState = transactionalState.State;
                Assert.NotSame(state, newState);
                Assert.False(newState.IsLocked);
                Assert.Equal(state.Value, newState.Value);
                patchedState = transactionalState.Close();
                Assert.Same(newState, patchedState);
            }

            Assert.True(patchedState.IsLocked);
        }
    }
}
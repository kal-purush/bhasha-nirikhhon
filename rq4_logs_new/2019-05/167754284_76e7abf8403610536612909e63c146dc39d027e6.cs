// ----------------------------------------------------------------------
// <copyright file="DelayActionBehavior.cs" company="SoloX Software">
// Copyright (c) SoloX Software. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using SoloX.ActionDispatch.Core.Action;
using SoloX.ActionDispatch.Core.Dispatch;
using SoloX.ActionDispatch.Core.Sample.State.Basic;

namespace SoloX.ActionDispatch.Core.ITest.Dipatch.Behavior
{
    public class DelayActionBehavior : IActionBehaviorAsync<IStateA, IStateA>
    {
        public DelayActionBehavior(int delay, IActionBehavior<IStateA, IStateA> behavior)
        {
            this.Delay = delay;
            this.Behavior = behavior;
        }

        public int Delay { get; }

        public IActionBehavior<IStateA, IStateA> Behavior { get; }

        public async Task Apply(IDispatcher<IStateA> dispatcher, IStateA state)
        {
            await Task.Delay(this.Delay).ConfigureAwait(false);

            dispatcher.Dispatch(this.Behavior, s => s);
        }
    }
}
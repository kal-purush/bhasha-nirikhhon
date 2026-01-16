// ----------------------------------------------------------------------
// <copyright file="ExampleAsyncActionBehavior.cs" company="SoloX Software">
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
using SoloX.ActionDispatch.Examples.State;

namespace SoloX.ActionDispatch.Examples.ActionBehavior
{
    /// <summary>
    /// Example asynchronous action.
    /// </summary>
    public class ExampleAsyncActionBehavior : IActionBehaviorAsync<IExampleAppState, IExampleChildState>
    {
        /// <inheritdoc/>
        public async Task Apply(IDispatcher<IExampleAppState> dispatcher, IExampleChildState state)
        {
            await Task.Delay(2000).ConfigureAwait(false);

            dispatcher.Dispatch(new ExampleActionBehavior(2), s => s.ChildState);
        }
    }
}
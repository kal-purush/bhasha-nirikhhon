// ----------------------------------------------------------------------
// <copyright file="IUnhandledExceptionBehavior.cs" company="SoloX Software">
// Copyright (c) SoloX Software. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Text;
using SoloX.ActionDispatch.Core.State;

namespace SoloX.ActionDispatch.Core.Action
{
    /// <summary>
    /// Unhandled exception behavior that will be used if an exception is thrown while dispatching an action.
    /// </summary>
    /// <typeparam name="TRootState">The state root type.</typeparam>
    public interface IUnhandledExceptionBehavior<TRootState> : IActionBehavior<TRootState, TRootState>
        where TRootState : IState
    {
        /// <summary>
        /// Gets the unhandled exception.
        /// </summary>
        Exception Exception { get; }
    }
}
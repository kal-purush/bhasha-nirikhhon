// ----------------------------------------------------------------------
// <copyright file="IRelativeDispatcher.cs" company="SoloX Software">
// Copyright (c) SoloX Software. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for full license information.
// </copyright>
// ----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using SoloX.ActionDispatch.Core.Action;
using SoloX.ActionDispatch.Core.State;

namespace SoloX.ActionDispatch.Core.Dispatch
{
    /// <summary>
    /// IRelativeDispatcher interface. Used to dispatch IAction from an asynchronous action that will change
    /// the root state object hierarchy.
    /// </summary>
    /// <typeparam name="TRootState">Type of the root state object on witch actions will apply.</typeparam>
    /// <typeparam name="TIntermediatState">Type of the intermediate state object on witch actions will apply.</typeparam>
    public interface IRelativeDispatcher<TRootState, TIntermediatState>
        where TRootState : IState<TRootState>
        where TIntermediatState : IState
    {
        /// <summary>
        /// Gets the base dispatcher.
        /// </summary>
        IDispatcher<TRootState> Dispatcher { get; }

        /// <summary>
        /// Dispatch an action on a current state.
        /// </summary>
        /// <typeparam name="TState">The target state type.</typeparam>
        /// <param name="actionBehavior">The action behavior to apply.</param>
        /// <param name="selector">The target state selector expression.</param>
        void Dispatch<TState>(IActionBehavior<TRootState, TState> actionBehavior, Expression<Func<TIntermediatState, TState>> selector)
            where TState : IState<TState>;

        /// <summary>
        /// Dispatch an action on a current state.
        /// </summary>
        /// <typeparam name="TState">The target state type.</typeparam>
        /// <param name="actionBehavior">The asynchronous action behavior to apply.</param>
        /// <param name="selector">The target state selector expression.</param>
        void Dispatch<TState>(IActionBehaviorAsync<TRootState, TState> actionBehavior, Expression<Func<TIntermediatState, TState>> selector)
            where TState : IState<TState>;
    }
}
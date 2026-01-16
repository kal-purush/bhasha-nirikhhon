// <copyright file="CancellationTokenSourceExtensions.cs" company="JSSoft">
//   Copyright (c) 2024 Jeesu Choi. All Rights Reserved.
//   Licensed under the MIT License. See LICENSE.md in the project root for license information.
// </copyright>

#if !NET8_0_OR_GREATER
#pragma warning disable

using System.Threading;
using System.Threading.Tasks;

namespace JSSoft.Communication;

internal static class CancellationTokenSourceExtensions
{
    public static Task CancelAsync(this CancellationTokenSource @this)
    {
        return Task.Run(() => @this.Cancel());
    }

}

#endif // !NET8_0_OR_GREATER
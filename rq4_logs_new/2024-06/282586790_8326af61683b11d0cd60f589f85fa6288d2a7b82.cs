// <copyright file="User.cs" company="JSSoft">
//   Copyright (c) 2024 Jeesu Choi. All Rights Reserved.
//   Licensed under the MIT License. See LICENSE.md in the project root for license information.
// </copyright>

using System;
using JSSoft.Communication.Services;

namespace JSSoft.Communication.Server.Services;

internal sealed class User
{
    public string UserId { get; set; } = string.Empty;

    public string UserName { get; set; } = string.Empty;

    public string Password { get; set; } = string.Empty;

    public Authority Authority { get; set; }

    public Guid Token { get; set; }
}
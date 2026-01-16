// Copyright (c) All contributors. All rights reserved. Licensed under the MIT license.

using System.Threading;

namespace Arc.Threading;

/// <summary>
/// <see cref="MonitorLock"/> class implements <see cref="ILockable"/>, which is actually a wrapper class for an object and <see cref="Monitor"/> methods.
/// </summary>
public class MonitorLock : ILockable
{
    public bool IsLocked
        => Monitor.IsEntered(this.syncObject);

    public bool Enter()
    {
        var lockTaken = false;
        Monitor.Enter(this.syncObject, ref lockTaken);
        return lockTaken;
    }

    public void Exit()
    {
        Monitor.Exit(this.syncObject);
    }

    private object syncObject = new();
}
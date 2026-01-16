namespace TaskManager.Task.Interfaces
{
    using System;

    public interface ISubscriber
    {
        void OnReminderTimeReached(object sourse, EventArgs e);
    }
}
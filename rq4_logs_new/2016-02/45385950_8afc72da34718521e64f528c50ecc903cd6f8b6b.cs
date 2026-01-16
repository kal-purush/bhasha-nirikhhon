using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace AntWars.Helper
{
    /// <summary>
    /// See http://stackoverflow.com/questions/961869/is-there-a-synchronization-class-that-guarantee-fifo-order-in-c
    /// </summary>
    public sealed class QueuedLock
    {
        private object innerLock;
        private volatile int ticketsCount = 0;
        private volatile int ticketToRide = 1;

        public QueuedLock()
        {
            innerLock = new Object();
        }

        public void Enter()
        {
            int myTicket = Interlocked.Increment(ref ticketsCount);
            Monitor.Enter(innerLock);
            while (true)
            {
            
                if (myTicket == ticketToRide)
                {
                    return;
                }
                else
                {
                    Monitor.Wait(innerLock);
                }
            }
        }

        public void Exit()
        {
            Interlocked.Increment(ref ticketToRide);
            Monitor.PulseAll(innerLock);
            Monitor.Exit(innerLock);
        }
    }
}
using System;
using System.Threading;
using System.Diagnostics;
using System.Collections.Generic;

namespace Dining_Philosophers
{
	class MainClass
	{
		// the number of philosophers
		public const int NUM_PHILOSOPHERS = 5;

		// maximum amount of time spent thinking, in milliseconds
		public const int THINK_TIME = 10;

		// maximum amount of time spent eating, in milliseconds
		public const int EAT_TIME = 10;

		// the timeout for locking a chopstick, in milliseconds
		public const int LOCK_TIMEOUT = 15;

		// recovery time when a chopstick lock fails
		public const int RECOVERY_TIME = 15;

		// total program runtime in milliseconds
		public const int RUN_TIME = 10000;

		// the chopsticks, implemented as sync objects
		public static object[] chopstick = new object[NUM_PHILOSOPHERS];

		// the philosophers, implemented as threads
		public static Thread[] philosopher = new Thread[NUM_PHILOSOPHERS];

		// a shared stopwatch to abort the program after a specific timeout
		public static Stopwatch stopwatch = new Stopwatch ();

		// a shared dictionary to measure how many philosophers have eaten
		public static Dictionary<int, int> eatingTime = new Dictionary<int, int> ();

		// a sync object to protect access to the eating field
		public static object eatingSync = new object ();

		// this method lets the philosopher think
		public static void Think ()
		{
			Random random = new Random ();
			Thread.Sleep (random.Next (THINK_TIME));
		}

		// this method lets the philosopher eat
		public static void Eat (int index)
		{
			Random random = new Random ();
			int time_spent_eating = random.Next (EAT_TIME);
			Thread.Sleep (time_spent_eating);

			// log our total eating time
			lock (eatingSync)
			{
				eatingTime [index] += time_spent_eating;
			}
		}

		// the philosopher work method - think and eat until timeout
		public static void DoWork (int index, object chopstick1, object chopstick2)
		{
			do
			{
				bool lockTaken1 = false;
				bool lockTaken2 = false;

				// only think after a successful prior eat, otherwise retry eat
				if (lockTaken1 && lockTaken2)
				{
					Think ();
				}

				try
				{
					// lock first chopstick, random sleep if lock times out
					Monitor.TryEnter (chopstick1, LOCK_TIMEOUT, ref lockTaken1);
					if (lockTaken1)
					{
						try
						{
							// lock second chopstick, random sleep if lock times out
							Monitor.TryEnter (chopstick2, LOCK_TIMEOUT, ref lockTaken2);
							if (lockTaken2)
							{
								Eat (index);
							}
							else
							{
								Random random = new Random ();
								int time_spent_eating = random.Next (RECOVERY_TIME);
								Thread.Sleep (time_spent_eating);
							}
						}
						finally
						{
							if (lockTaken2)
								Monitor.Exit (chopstick2);
						}
					}
					else
					{
						Random random = new Random ();
						int time_spent_eating = random.Next (RECOVERY_TIME);
						Thread.Sleep (time_spent_eating);
					}
						
				}
				finally
				{
					if (lockTaken1)
						Monitor.Exit (chopstick1);
				}
			}
			while (stopwatch.ElapsedMilliseconds < RUN_TIME);
		}


		public static void Main (string[] args)
		{
			// set up the dictionary that measures total eating time
			for (int i = 0; i < NUM_PHILOSOPHERS; i++)
			{
				eatingTime.Add (i, 0);
			}

			// set up the chopsticks
			for (int i = 0; i < NUM_PHILOSOPHERS; i++)
			{
				chopstick [i] = new object ();
			}

			// set up the philosophers
			for (int i = 0; i < NUM_PHILOSOPHERS; i++)
			{
				int index = i;
				object chopstick1 = chopstick [i];
				object chopstick2 = chopstick [(i + 1) % NUM_PHILOSOPHERS];
				philosopher [i] = new Thread (
					_ =>
					{
						DoWork (index, chopstick1, chopstick2);
					}
				);
			}

			// start the philosophers
			stopwatch.Start ();
			Console.WriteLine ("Starting philosophers...");
			for (int i = 0; i < NUM_PHILOSOPHERS; i++)
			{
				philosopher [i].Start ();
			}

			// wait for all philosophers to complete
			for (int i = 0; i < NUM_PHILOSOPHERS; i++)
			{
				philosopher [i].Join ();
			}
			Console.WriteLine ("All philosophers have finished");

			// report the total time spent eating
			int total_eating_time = 0;
			for (int i = 0; i < NUM_PHILOSOPHERS; i++)
			{
				Console.WriteLine ("Philosopher {0} has eaten for {1}ms", i, eatingTime [i]);
				total_eating_time += eatingTime [i];
			}
			Console.WriteLine ("Total time spent eating: {0}ms", total_eating_time);
			Console.WriteLine ("Optimal time spent eating: {0}ms", stopwatch.ElapsedMilliseconds * 2);



		}
	}
}
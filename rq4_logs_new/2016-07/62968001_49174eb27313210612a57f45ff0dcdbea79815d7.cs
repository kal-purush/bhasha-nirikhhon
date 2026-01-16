using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;

namespace ReaderWriterLockTest
{
    class Program
    {
        static Random r = new Random();
        const int ElementCount = 10;
        static int[] Table = new int[ElementCount];

        const int WriterThreadCount = 2;
        const int ReaderThreadCount = 10;

        //maximum timeouts
        const int MaxTimeBetweenReadOperations = 1000; //1s
        const int MaxTimeBetweenWriteOperations = 10000; //10s
        const int MaxReadOperationDuration = 1000; //1s 
        const int MaxWriteOperationDuration = 100; //0.1s

        static ReaderWriterLockSlim _readerWriterLock = new ReaderWriterLockSlim();

        static void ModifyElement(int index, int? value = null)
        {
            _readerWriterLock.EnterWriteLock();
            Console.WriteLine("Thread waiting for write : {0}, thread waiting for read : {1}", _readerWriterLock.WaitingWriteCount, _readerWriterLock.WaitingReadCount);
            try
            {
                if (value.HasValue) Table[index] = value.Value;
                else Table[index]++;
                Console.WriteLine("Element {0} was modified in thread no. {1}", index.ToString(),
                    Thread.CurrentThread.ManagedThreadId);
                Thread.Sleep(r.Next(MaxWriteOperationDuration));
            }
            catch (Exception exc)
            {
                Console.WriteLine("Modification of element {0} in thread {1} is impossible # {2}", index.ToString(),
                    Thread.CurrentThread.ManagedThreadId, exc.Message);
            }
            finally
            {
                _readerWriterLock.ExitWriteLock();
            }
        }

        static int ReadElement(int index)
        {
            int result = -1;
            _readerWriterLock.EnterReadLock();
            Console.WriteLine("Current number of reading threads : {0}, threads waiting for write : {1}", _readerWriterLock.CurrentReadCount, _readerWriterLock.WaitingWriteCount);
            try
            {
                result = Table[index];
                Console.WriteLine("Element {0} = {1}", index, result);
                Thread.Sleep(r.Next(MaxReadOperationDuration));
                return result;
            }
            catch (Exception exc)
            {
                Console.WriteLine("Reading element {0} in thread {1} is impossible # {2}", index.ToString(),
                    Thread.CurrentThread.ManagedThreadId, exc.Message);
                return result;
            }
            finally
            {
                _readerWriterLock.ExitReadLock();
            }
        }

        private static void DisplayTable()
        {
            Console.WriteLine("###--Table Content--###");
            foreach (int element in Table)
            {
                Console.WriteLine(element);
            }
            Console.WriteLine("###--End of Content--###");
        }
        static void Main(string[] args)
        {
            for (int i = 0; i < ElementCount; i++) Table[i] = 0;
            DisplayTable();
            Console.WriteLine("Press Enter to start");
            Console.WriteLine("Then press enter to stop");
            Console.ReadLine();
            ThreadStart writerAction = () =>
            {
                Thread.Sleep(r.Next(MaxTimeBetweenWriteOperations));
                while (true)
                {
                    try
                    {
                        Console.WriteLine("Preparing to element modification MTID:{0}",
                            Thread.CurrentThread.ManagedThreadId);
                        int index = r.Next(ElementCount);
                        ModifyElement(index, r.Next() % 10000);
                    }
                    catch (ThreadAbortException)
                    {
                        Console.WriteLine("Writer Thread MTID:{0} aborted", Thread.CurrentThread.ManagedThreadId);
                    }
                }
            };

            ThreadStart readerAction = () =>
            {
                Thread.Sleep(r.Next(MaxTimeBetweenReadOperations));
                while (true)
                {
                    try
                    {
                        Console.WriteLine("Preparing to read element MTID:{0}", Thread.CurrentThread.ManagedThreadId);
                        int index = r.Next(ElementCount);
                        int value = ReadElement(index);
                        Console.WriteLine("Element {0} = {1}, MTID:{2}", index, value,
                            Thread.CurrentThread.ManagedThreadId);
                        Thread.Sleep(MaxTimeBetweenReadOperations);
                    }
                    catch (ThreadAbortException exc)
                    {
                        Console.WriteLine("Reader Thread MTID:{0} aborted", Thread.CurrentThread.ManagedThreadId);
                    }
                }
            };

            Thread[] writers = new Thread[WriterThreadCount];
            for (int i = 0; i < WriterThreadCount; i++)
            {
                writers[i] = new Thread(writerAction);
                writers[i].IsBackground = true;
                writers[i].Start();
            }

            Thread[] readers = new Thread[ReaderThreadCount];
            for (int i = 0; i < ReaderThreadCount; i++)
            {
                readers[i] = new Thread(readerAction);
                readers[i].IsBackground = true;
                readers[i].Start();
            }
            Console.ReadLine();
            Console.WriteLine("Aborting...");
            for (int i = 0; i < WriterThreadCount; i++)
            {
                Console.WriteLine("Writer {0} MTID:{1} aborting...", i, writers[i].ManagedThreadId);
                writers[i].Abort();
            }
            for (int i = 0; i < ReaderThreadCount; i++)
            {
                Console.WriteLine("Reader {0} MTID:{1} aborting...", i, readers[i].ManagedThreadId);
                readers[i].Abort();
            }
            Console.ReadLine();
        }
    }
}
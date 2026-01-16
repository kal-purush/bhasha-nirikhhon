using System;
using System.Collections.Generic;

namespace FillRoomWithBoxes.App
{
    public class Program
    {
        /// <summary>
        /// Fill the room with boxes of give sizes
        /// </summary>
        /// <param name="roomSize">Size of the room</param>
        /// <param name="possibleSizes">Box sizes</param>
        /// <param name="boxes">All boxes which fit in the room</param>
        public static void FillRoomWithBoxes(int roomSize, List<int> possibleSizes, ref List<int> boxes)
        {
            int getMaxBoxSize(List<int> sizes, int currentSize)
            {
                foreach (int item in sizes)
                {
                    if (currentSize - item >= 0)
                        return item;
                }
                throw new Exception("Unable to fit boxes into the room");
            }
            if (roomSize == 0)
                return;
            int boxAvailable = getMaxBoxSize(possibleSizes, roomSize);
            boxes.Add(boxAvailable);
            roomSize = roomSize - boxAvailable;
            FillRoomWithBoxes(roomSize, possibleSizes, ref boxes);

        }
        static void Main(string[] args)
        {
            List<int> possibleSizes = new List<int> { 7, 3, 1 };
            int roomSize = 25;
            List<int> boxes = new List<int>();
            FillRoomWithBoxes(roomSize, possibleSizes, ref boxes);
            Console.WriteLine($"Tries to fill room of size '{roomSize}' with boxes '{String.Join(", ", possibleSizes.ToArray())}'. Result: '{String.Join(", ", boxes.ToArray())}'");
        }
    }
}
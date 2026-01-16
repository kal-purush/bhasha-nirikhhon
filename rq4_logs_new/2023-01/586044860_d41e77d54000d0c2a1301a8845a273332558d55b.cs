using Engine.Models.Moves;
using Engine.Sorting.Comparers;
using System.Runtime.CompilerServices;

namespace Engine.Sorting
{
    public static  class SortExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void InsertionSort(this MoveBase[] items, int count)
        {
            for (int i = 1; i < count; ++i)
            {
                var key = items[i];
                int j = i - 1;

                // Move elements of arr[0..i-1],
                // that are greater than key,
                // to one position ahead of
                // their current position
                while (j >-1 && key.IsGreater(items[j]))
                {
                    items[j + 1] = items[j];
                    j--;
                }
                items[j + 1] = key;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void ArraySort(this MoveBase[] items, int count)
        {
            Array.Sort(items,0, count);
        }
    }
}
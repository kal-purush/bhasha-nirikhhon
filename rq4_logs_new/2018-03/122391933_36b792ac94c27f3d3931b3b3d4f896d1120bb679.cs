using System.Collections;
using System.Collections.Generic;
using UnityEngine;
using System;

public class PriorityQ<T> where T : IPQItem<T>
{
    T[] items;
    int itemCount;

    public PriorityQ(int size)
    {
        items = new T[size];
    }
    public void Add( T item )
    {
        item.QIndex = itemCount;
        items[itemCount] = item;
        SortUp(item);
        itemCount++;
    }
    private void SortUp( T item )
    {
        int parentIndex = (item.QIndex - 1) / 2;
        while( true )
        {
            T parent = items[parentIndex];
            if (item.CompareTo(parent) > 0 )
            {
                SwapItems( item, parent);
            }
            else
            {
                break;
            }
            parentIndex = (item.QIndex - 1) / 2;
        }
    }
    private void SortDown( T item )
    {
        while( true)
        {
            int leftChildIndex = item.QIndex * 2 + 1;
            int rightChildIndex = item.QIndex * 2 + 2;
            int swapIndex = 0;
            if( leftChildIndex < itemCount )
            {
                swapIndex = leftChildIndex;
                if( rightChildIndex < itemCount )
                {
                    if( items[leftChildIndex].CompareTo(items[rightChildIndex]) < 0 )
                    {
                        swapIndex = rightChildIndex;
                    }
                }

                if( item.CompareTo(items[swapIndex]) < 0 )
                {
                    SwapItems(item, items[swapIndex]);
                }
                else
                {
                    return;
                }
            }
            else
            {
                return;
            }
        }
    }

    private void SwapItems( T first, T second )
    {
        items[first.QIndex] = second;
        items[second.QIndex] = first;
        int firstIndex = first.QIndex;
        first.QIndex = second.QIndex;
        second.QIndex = firstIndex;

    }
    public T ExtractFirstItem()
    {
        T item = items[0];
        itemCount--;
        items[0] = items[itemCount];
        items[0].QIndex = 0;
        SortDown(items[0]);
        return item;
    }
    public bool IsItemInQ( T item )
    {
        return Equals(items[item.QIndex], item);
    }
    public int getSize()
    {
        return itemCount;
    }
    public void Update( T item )
    {
        SortUp(item);
    }
}
public interface IPQItem<T> : IComparable<T>
{
    int QIndex
    {
        get;
        set;
    }
}
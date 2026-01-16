using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace BouncingRectangles.Server.Models
{
    [DebuggerDisplay("Capacity: {Items.Capacity}, Count: {Items.Count}, Id: {Id}")]
    public class Group<T>
    {
        public Guid Id { get; }
        public List<T> Items { get; }

        public Group(Guid id, int capacity)
        {
            Id = id;
            Items = new List<T>(capacity);
        }
    }
}
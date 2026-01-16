using System;
using Xunit;
using StacksQueues.Classes;
using LinkedList.Classes;

namespace QueuesTests
{
    public class Enqueue
    {
        [Fact]
        public void EnqueueOnEmptyQueue()
        {
            Queues queue = new Queues();
            Node node = new Node(1);
            queue.Enqueue(node);
            Assert.True(queue.front == node);
            Assert.True(queue.rear == node);
        }
        [Fact]
        public void EnqueueOnQueue()
        {
            Queues queue = new Queues();
            Node nodeOne = new Node(1);
            Node nodeTwo = new Node(2);

            queue.Enqueue(nodeOne);
            queue.Enqueue(nodeTwo);

            Assert.True(queue.front == nodeOne);
            Assert.True(queue.rear == nodeTwo);
        }
    }
    public class Dequeue
    {
        [Fact]
        public void DequeueOnEmptyQueue()
        {
            Queues queue = new Queues();
            Assert.Throws<NullReferenceException>(() => queue.Dequeue());
        }
        [Fact]
        public void DequeueOnQueue()
        {
            Queues queue = new Queues();
            Node nodeOne = new Node(1);
            Node nodeTwo = new Node(2);

            queue.Enqueue(nodeOne);
            queue.Enqueue(nodeTwo);
            Assert.True(queue.Dequeue().Value == nodeOne.Value);
            Assert.True(queue.Dequeue().Value == nodeTwo.Value);
            Assert.Throws<NullReferenceException>(() => queue.Dequeue());
        }
    }
    public class Peak
    {
        public void PeakOnEmptyQueue()
        {
            Queues queue = new Queues();
            Assert.Null(queue.Peak());
        }
        public void PeakOnQueue()
        {
            Queues queue = new Queues();
            Node nodeOne = new Node(1);
            Node nodeTwo = new Node(2);
            Node nodeThree = new Node(3);
            Node nodeFour = new Node(4);

            queue.Enqueue(nodeOne);
            queue.Enqueue(nodeTwo);
            queue.Enqueue(nodeThree);
            queue.Enqueue(nodeFour);

            Assert.Equal(1, queue.Peak().Value);
        }
    }
}
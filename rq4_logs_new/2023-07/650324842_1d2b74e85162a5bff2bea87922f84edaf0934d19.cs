namespace LinkedListCycle;

public class Node
{
    public int Value { get; init; }

    public Node? Next { get; set; }
}

public class SinglyLinkedList
{
    private Node? Head { get; set; }

    public void Init()
    {
        var random = new Random();
        Node? tempNode = null;

        for (var i = 0; i < random.Next(6, 20); i++)
        {
            Node newNode = new() { Value = random.Next(1, 99) };
            if (Head == null)
            {
                Head = newNode;
                tempNode = Head;
            }

            tempNode.Next = newNode;
            tempNode = newNode;
        }
    }
    
    public void Traverse()
    {
        var pointer = Head;
        while (pointer != null)
        {
            Console.Write($"{pointer.Value}{(pointer.Next != null ? "->" : "")}");
            pointer = pointer.Next;
        }
        Console.Write("\n");
    }

    public void AddCycleToHead()
    {
        var pointer = Head;

        while (pointer?.Next != null)
        {
            pointer = pointer.Next;
        }

        pointer.Next = Head;
    }
    
    public void AddCycleToLast()
    {
        var pointer = Head;

        while (pointer.Next != null)
        {
            pointer = pointer.Next;
        }

        pointer.Next = pointer;
    }

    public void CheckForCycle()
    {
        var slowPointer = Head;
        var fastPointer = Head;

        while (slowPointer?.Next != null || fastPointer?.Next != null)
        {
            fastPointer = fastPointer?.Next?.Next;
            slowPointer = slowPointer?.Next;

            if (fastPointer == slowPointer)
            {
                Console.WriteLine($"Cycle Found! Element: {slowPointer?.Value} -> {slowPointer?.Next?.Value}");
                return;
            }
        }
        
        Console.WriteLine("Cycle haven't been found");
    }
}
class Patient
{
    public int id;
    public int priority { get; set; }
    public Patient(int id, int priority)
    {
        this.id = id;
        this.priority = priority;
    }
}
class PriorQueue(Queue<Patient> q)
{
    Queue<Patient> q = q;

    public void PriorInsert(Patient p)
    {
        Queue<Patient> temp = new();
        while (q.Peek().priority >= p.priority)
        {
            temp.Enqueue(q.Dequeue());
        }
        q.Enqueue(p);
        while (temp.Count > 0)
        {
            q.Enqueue(temp.Dequeue());
        }
    }
    public void Update(int id, int pri)
    {
        Patient p = new(id, pri);
        PriorInsert(p);
        Queue<Patient> temp = new();
        while (q.Peek().id != id)
        {
            temp.Enqueue(q.Dequeue());
        }
        q.Dequeue();
        while (temp.Count > 0)
        {
            q.Enqueue(temp.Dequeue());
        }
    }
}
class Program { }
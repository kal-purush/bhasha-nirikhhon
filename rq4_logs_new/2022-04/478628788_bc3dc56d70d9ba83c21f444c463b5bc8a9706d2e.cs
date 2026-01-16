using System.Collections;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}
app.UseHttpsRedirection();

//Most importants data structures
//Linked List, HashTable, Stack and Queues,
            
//Linked List - > When i need insertion/deletions in realtime and don´t need random acess
// If you need a random acess or different operators, use just a List<> 

string[] classes = {"Warrior", "Mage", "Druid"};
LinkedList<String> data = new LinkedList<string>(classes);
data.AddLast("Warlock");
data.AddFirst("Paladin");

app.MapGet("/linkedlist", () =>
{ 
    return data;
});

// Example 'https://localhost:7223/linkedlist?n=hunter&m=wizzard
app.MapPost("/linkedlist", (string n, string m) =>
{  
    data.AddLast(m);
    data.AddFirst(n);
    return data;
});


//HashTable -> when i need a optimezed lookpups by computing hashcode keys 
//(Remember as a optimized dictionary)

Hashtable datahash = new Hashtable();
datahash.Add(1,"Goblin");
datahash.Add(2,"Skeleton");
datahash.Add(3,"Orc");

app.MapGet("/hash", () =>
{
   return datahash;
});

//Example https://localhost:7223/hash?n=4&m=Dragon
app.MapPost("/hash", (int n, string m) =>
{
    datahash.Add(n,m);
    return datahash;
});


//Stacks, Last-in, first out structures
Stack datastack = new Stack();

datastack.Push("Village");
datastack.Push("Cave");
datastack.Push("Castle");

app.MapGet("/stack", () =>
{
  return datastack;
});

// Example https://localhost:7223/stack?n=Dungeon
app.MapPost("/stack", (string n) =>
{
    datastack.Pop();
    datastack.Push(n);
    return datastack;
});

//Queue, the first element entered first out

Queue dataqueue = new Queue();

dataqueue.Enqueue("Life Regen effect");
dataqueue.Enqueue("blindness");
dataqueue.Enqueue("mana Regen effect");

app.MapGet("/queue", () =>{
    return dataqueue;
});

app.MapPost("/queue", (string n) =>{
    dataqueue.Dequeue();
    dataqueue.Enqueue(n);
    return dataqueue;
});

app.Run();
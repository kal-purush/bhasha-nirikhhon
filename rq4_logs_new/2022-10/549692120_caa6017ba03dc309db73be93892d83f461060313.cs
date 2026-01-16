int count = 0;
int distance = 10000;
int firstFriendSpeed = 1;
int secondFriendSpeed = 2;
int dogSpeed = 5;
bool friend2 = true;
int time;

while(distance > 10)
{
    if(friend2 == false)
    {
        time = distance / (firstFriendSpeed + dogSpeed);
        friend2 = true;
    }
    else
    {
        time = distance / (secondFriendSpeed + dogSpeed);
        friend2 = false;
    }

    distance = distance - (firstFriendSpeed + secondFriendSpeed) * time;
    count = count + 1;
}

Console.Write("count = ");
Console.WriteLine(count);
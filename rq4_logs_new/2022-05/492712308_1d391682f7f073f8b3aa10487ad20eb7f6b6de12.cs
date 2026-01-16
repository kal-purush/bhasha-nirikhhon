// (1) Write a program and ask the user to enter a number. The number should be between 1 to 10.
// If the user enters a valid number, display "Valid" on the console. Otherwise, display
// "Invalid". (This logic is used a lot in applications where values entered into input boxes
// need to be validated.)
Console.WriteLine("Please enter a value between 0 and 10");
var userInput = Console.ReadLine();

var input = Convert.ToInt32(userInput);
var validation = input > 1 && input < 10 ? "Valid" : "Invalid";

Console.WriteLine(validation);

// (2) Write a program which takes two numbers from the console and displays the maximum of the two.
Console.WriteLine("Please enter your first number");
var firstUserInput = Console.ReadLine();
Console.WriteLine("Please enter your last number");
var secondUserInput = Console.ReadLine();

var firstInput = Convert.ToInt32(firstUserInput);
var secondInput = Convert.ToInt32(secondUserInput);
var maxValue = firstInput > secondInput ? firstInput : secondInput;

Console.WriteLine(maxValue);

// (3) Write a program and ask the user to enter the width and height of an image.
// Then tell if the image is landscape or portrait.
Console.WriteLine("Please enter the width of the image");
var widthUserInput = Console.ReadLine();
Console.WriteLine("Please enter the height of the image");
var heightUserInput = Console.ReadLine();

var width = Convert.ToInt32(widthUserInput);
var height = Convert.ToInt32(heightUserInput);

if (width > height)
{
    Console.WriteLine("Landscape");
}
else if (width < height)
{
    Console.WriteLine("Portrait");
}
else
{
    Console.WriteLine("The ratio of the image is 1:1");
}

// (4) Your job is to write a program for a speed camera. For simplicity, ignore
// the details such as camera, sensors, etc and focus purely on the logic. Write
// a program that asks the user to enter the speed limit. Once set, the program
// asks for the speed of a car. If the user enters a value less than the speed limit,
// program should display Ok on the console. If the value is above the speed limit,
// the program should calculate the number of demerit points. For every 5km/hr above
// the speed limit, 1 demerit points should be incurred and displayed on the console.
// If the number of demerit points is above 12, the program should display License Suspended.
Console.WriteLine("Please enter the speed limit");
var speedLimitUserInput = Console.ReadLine();
Console.WriteLine("What was the speed of the car?");
var vehicleSpeedUserInput = Console.ReadLine();

var speedLimit = Convert.ToInt32(speedLimitUserInput);
var vehicleSpeed = Convert.ToInt32(vehicleSpeedUserInput);
var demeritPoints = (vehicleSpeed - speedLimit) / 5;

if (speedLimit > vehicleSpeed)
{
    Console.WriteLine("OK");
}
else if (demeritPoints > 12)
{
    Console.WriteLine("License Suspended");
}
else
{
    demeritPoints = (vehicleSpeed - speedLimit) / 5;
    Console.WriteLine($"Demerit Points: {demeritPoints}");
}
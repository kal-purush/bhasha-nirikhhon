//1. Looping a Triangle
/*Expected Input: None.
Expected Output: A triangle of #'s 7 rows tall
Expected Behavior: Print a triangle of #'s, beginning with one at the top incrementing by one each row down
 */

for(var str = ""; str.length < 8; str += "*")
{
console.log(str);    
}


//2. FizzBuzz
/*Expected Input: None.
Expected Output: A list of numbers and the strings "Fizz" or "Buzz".
Expected Behavior: Print a list from 1 - 100 printing fizz instead
of numbers divisible by 3, and buzz for numbers divisible by 5.

Add functionality: Print FizzBuzz for 
numbers divisible by both 3 and 5.
 */
function FizzBuzz()
{

    for(var i = 1; i < 101; i++)
    {
        if(i % 3 == 0)
        {
            console.log("Fizz");
        } else if(i % 5 == 0){
            console.log("Buzz");
        } else {
            console.log(i);
        }
        
    }

}

//3. Chess Board
/*Expected Input: Integer representing the size of the board.
Expected Output: A chessboard of the size given.
Expected Behavior: Print a chessboard of the given size by looping through the
appropriate amount of times.
 */

 function ChessBoard(bSize)
 {
    for(let i = 0; i < bSize; i++)
    {
      var line = "";
        for(let j = 0; j < bSize; j++)
        {
            if(i % 2 == 0) //if(even)
            {
                line += "# ";
            } else if(i % 2 == 1){ //if(odd)
                line += " #";
            }
        }
        console.log(line);
    }

}
var sum = 0;

for(i = 1; i < 1000; i++)
{
  if (i % 5 === 0)
  {
  	var sum = sum + i;
  }
  else if (i % 3 === 0)
  {
  	var sum = sum + i;
  }
}

console.log(sum);
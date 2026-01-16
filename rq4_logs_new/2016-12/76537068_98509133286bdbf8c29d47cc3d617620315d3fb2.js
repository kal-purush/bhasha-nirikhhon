
function truncateString(str, num) {
   var dots = "...";
  
  if (str.length > num)
      {
        if (num <= 3)
        {
          str = str.slice(0, num);
          str += dots;
        }
        
        else
        {
          str = str.slice(0, num - dots.length);
          str += dots;
        }  
      }
   
  return str;
}

truncateString("A-tisket a-tasket A green and yellow basket", 11);


function fn1(){
    //Storing the input text int string str 
    var str = document.getElementById("text1").value;
  
    //finding length of string
    var n = str.length;

    // creating an empty string to store the result
    let res = "";
    //Iterating through each element of string
    for (let i = 0; i < str.length; i++) {
       
       
       
        if(str[i]>='A' && str[i]<='Z')    //when the we have UpperCase letter 
        {
         
           var ascii_value = str.charCodeAt(i);  //Finding ascii value of that character
           
         
           ascii_value = ascii_value-65;
           ascii_value = 90-ascii_value;

           //Now converting from ascii value to character
            var character = String.fromCharCode(ascii_value);

            //appending that character in our result
            res+=character;
       
        }
        else if(str[i]>='a' && str[i]<='z'){           //When we have lowerCase letter
         
            //Finding ascii value of that character
            var ascii_value = str.charCodeAt(i);
           
           ascii_value = ascii_value-97;
           ascii_value = 122-ascii_value;

            //Now converting from ascii value to character
           var character = String.fromCharCode(ascii_value);

           
            //appending that character in our result
           res+=character;
          
    
        }
        else
        {
            //when we have characters other then english alphabets we will simply append it 
            res+=str[i];
        }

    }

    document.getElementById("Texts").innerHTML=res;
    


   

}
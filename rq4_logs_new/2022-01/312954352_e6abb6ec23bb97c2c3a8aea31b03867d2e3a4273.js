(()=>{
    var menu = document.querySelector('ul'),
        menulink =document.getElementById('menu'),
        sendlink =document.getElementById('send'),
        namePattern = /^[a-z,A-Z]{2,15}\ [a-z,A-Z]{2,15}$/, //for name validation
        emailPattern = /^[^ ]+@[^ ]+\.[a-z]{2,3}$/ , //for email validation 
        messagePattern =/^[A-Z][a-z,A-Z,0-9,_,.,\n, ]{50,}/; //for message validation
        
        /***************TOGGLE MENU************/
        menulink.addEventListener('click',(e)=>{
            menu.classList.toggle('active');
            e.preventDefault();
        })
        /***************SEND MESSAGE*************/
        sendlink.addEventListener('click',(e)=>{
            // alert("message sent, thank you!!!")
            
            var name = document.getElementById('fullname'),
                email = document.getElementById('email'),
                selection = document.getElementById('selection'),
                message = document.getElementById('message');
               
                //e.preventDefault();

                if( namePattern.test(name.value) && emailPattern.test(email.value) && messagePattern.test(message.value)){
                    if(selection.value ==="" || selection ===undefined || selection===null){
                        var option = document.createElement("option");
                        option.value = "getintouch";
                        selection.appendChild(option);
                    }
                    //console.log(`${name}\n${email}\n${selection.value}\n${message}`);
                    send();
                }else{
                    if(!namePattern.test(name.value)){
                        console.log("name invalid");
                        handleError(document.getElementById('validate-name'));
                    }
                    if(!emailPattern.test(email.value)){
                        console.log("email invalid");
                        handleError(document.getElementById('validate-email'));
                    }
                    if(!messagePattern.test(message.value)){
                        console.log("message too short");
                        handleError(document.getElementById('validate-message'));
                    }
                }

            function handleError(element){
                element.style.display ='block';

                setTimeout(()=>{
                    element.style.display = 'none';
                },2000);
            }

            function send(){
                //console.log(`${name} ${email} ${selection.value} ${message}`);
            
                Email.send({
                    SecureToken : "25ba687f-30d9-4d84-85d1-3a8c1f169040",
                    To : `info@godfreytc.co`,
                    From : `gtcsoft@godfreytc.co`,
                    Subject : `Website Message`,
                    Body : `Name: ${name.value}\nEmail: ${email.value}\nSelection:${selection.value}\nMessage:${message.value}`
                }).then(
                    sms =>{
                        if(sms ==="OK"){
                            name.value="";
                            email.value="";
                            message.value="";
                            alert("message sent");
                        }
                        else{
                            alert("something went wrong...try again");
                        }
                    }
                  );
                
            };
        })
    })();
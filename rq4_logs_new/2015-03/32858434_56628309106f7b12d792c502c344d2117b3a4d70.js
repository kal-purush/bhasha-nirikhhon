<!--
        
        function validation(mainForm)
        {
      /*   if ((mainForm.title.value == "")&&(mainForm.description.value == null)&&
             (mainForm.accept.checked== false))
            { 
            //alert("Please enter your title");
			//document.grtElementById("title").value=="";
			document.getElementById("title").style.backgroundColor = "red";
			document.getElementById("description").style.backgroundColor = "red";
			document.getElementById("rectngle").style.backgroundColor = "red";
			
			
		//	document.title.style.color = "blue";
			
			
			
			//var commentTag;
			
			//commentTag.style.backgroundColour = "#FFFF00";
		//	commentTag.style.borderWidth="3px";
            //document.mainForm.title.focus();   
            //return false; 
            }

*/
	

			if   (mainForm.title.value == "")
		            { 
		            //alert("Please enter your description");
					document.getElementById("title").style.backgroundColor = "red";

		            //document.mainForm.description.focus();   
		            //return false; 
		            }



         if   (mainForm.description.value.trim()=="")
	            { 
	            //alert("Please enter your description");
				document.getElementById("description").style.backgroundColor = "red";
	
	            //document.mainForm.description.focus();   
	            //return false; 
	            }
	
			if (mainForm.accept.checked== false)
					{
						//alert("Please accept the software license");
						document.getElementById("rectngle").style.backgroundColor = "red";

					//	return false;

					}
	
			if (mainForm.title.value != "")
			            { 
			            //alert("Please enter your title");

						document.getElementById("title").style.backgroundColor = "white";
					//	document.getElementById("description").style.backgroundColor = "black";
						//document.getElementById("rectngle").style.backgroundColor = "red";
	}
					if (mainForm.description.value.trim() != "")
			            { 
			            //alert("Please enter your title");

						document.getElementById("description").style.backgroundColor = "white";
					//	document.getElementById("description").style.backgroundColor = "black";
						//document.getElementById("rectngle").style.backgroundColor = "red";
	}
					if (mainForm.accept.checked!= false)
			            { 
			            //alert("Please enter your title");

						document.getElementById("rectngle").style.backgroundColor = "#EBF4FB";
					//	document.getElementById("description").style.backgroundColor = "black";
						//document.getElementById("rectngle").style.backgroundColor = "red";
	}
	
       /* if   (mainForm.continent.value == "Choose continent")
			    { 
			    // alert("Please enter your continent");
			    // document.mainForm.continent.focus();   
			    // return false; 
			    }
			
			if   (mainForm.country.value == "Choose country")
					    { 
					     //alert("Please enter your country");
					    // document.mainForm.country.focus();   
					    // return false; 
					    }
						if (mainForm.city.value == "")
				            { 
				           // alert("Please enter your city");
				           // document.mainForm.city.focus();   
				           // return false; 
				            }
				if ((mainForm.copyright[0].checked == false) && 
				(mainForm.copyright[1].checked == false))
				 {// alert("Please choose the copyright"); 
				//return false; 
				}
				
				if ((mainForm.c1.checked== false)&&(mainForm.c2.checked== false)&&
				(mainForm.c3.checked== false)&&(mainForm.c4.checked== false))
				{
					//alert("Please choose your Creative Commons Types");
				//	return false;
					
				}*/
			
        }
/* 
        function getCookie(Attribute) 
        // Assumes: Attribute is a string // Results: gets the value stored under the Attribute 
        { 
            
            if (document.cookie.indexOf(Attribute+"=") == -1) 
            { return ""; 
            }
            
            else 
            
            { var begin = document.cookie.indexOf(Attribute+"=") + Attribute.length+1;
              var end = document.cookie.indexOf(";", begin);
              if (end == -1) end = document.cookie.length; 
              return unescape(document.cookie.substring(begin, end)); 
            }
            
              
        }
       function setCookie(Attribute, Value) // Assumes: Attribute is a string 
        // Results: stores Value under the name Attribute, expires in 30 days 
        { var ExpireDate = new Date(); 
          ExpireDate.setTime(ExpireDate.getTime() + (30*24*3600*1000)); 
          document.cookie = Attribute + "=" + escape(Value) + "; expires=" + ExpireDate.toGMTString(); }
          */
         
           /*function Greeting()
// Results: displays greeting using cookie
        {
            visitCount = getCookie("visits");
            if (visitCount == "") 
            {
            setCookie("visits", 1);
            }
            else 
            {
            visitCount = parseFloat(visitCount)+1;
            //alert("Welcome back for visit #" + visitCount);
            setCookie("visits", visitCount);
            }
        }
          
        function ValidateForm(form) {

            depart=document.form.deparment.value;//object from select department in upload form
            levl=document.form.level.value;//object from select level in upload form
            course_name=document.form.coursename.value;
            file=document.form.fil.value;
            
            
            if (depart == "your select")
            {
                alert("Please Enter your department!!");
                document.form.deparment.focus();
                return false;
            }
            
            if (levl == "your select")
            {
                alert("Please Enter your level!!");
                document.form.level.focus();
                return false;
            }
            return true;
           if (course_name == "")
            {
                alert("Please Enter Course Name!!");
                document.form2.course_name.focus();
                return false;
            }
             
            if (file == "")
            {
                alert("Select File !!");
                document.form2.file.focus();
                return false;
            }
        } 
        function hide(element)
        {
            document.getElementById(element).style.visibility = "hidden";

        }
         function show(b)
        {
           if(document.getElementById(b).style.display == 'none')
           {
               document.getElementById(b).style.display = 'inline'; 
           }
           else 
           { 
                document.getElementById(b).style.display = 'none';
                
           } 
        }
            
function toggle_it(IS1,IS2,IS3,IS4,IS5,IS6,IS7,IS8,CS1,CS2,CS3,CS4,CS5,CS6,CS7,CS8,pr2)
        { 
           depart=document.upload_form.deparment.value;
           levl=document.upload_form.level.value;
           if(depart=="IS" && levl=="level 1" &&(document.getElementById(IS1).style.display == 'none'))
           {
               document.getElementById(IS1).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline'; 
           }
           else 
           { 
                document.getElementById(IS1).style.display = 'none';
                
           } 
           if(depart=="IS" && levl=="level 2" &&(document.getElementById(IS2).style.display == 'none'))
           {
               document.getElementById(IS2).style.display = 'inline'; 
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(IS2).style.display = 'none';
           } 
           if(depart=="IS" && levl=="level 3" &&(document.getElementById(IS3).style.display == 'none'))
           {
               document.getElementById(IS3).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(IS3).style.display = 'none'; 
           } 
           if(depart=="IS" && levl=="level 4" &&(document.getElementById(IS4).style.display == 'none'))
           {
               document.getElementById(IS4).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(IS4).style.display = 'none'; 
           } 
           if(depart=="IS" && levl=="level 5"&&(document.getElementById(IS5).style.display == 'none'))
           {
               document.getElementById(IS5).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(IS5).style.display = 'none';
           } 
           if(depart=="IS" && levl=="level 6" &&(document.getElementById(IS6).style.display == 'none'))
           {
               document.getElementById(IS6).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(IS6).style.display = 'none';
           } 
           if(depart=="IS" && levl=="level 7" &&(document.getElementById(IS7).style.display == 'none'))
           {
               document.getElementById(IS7).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(IS7).style.display = 'none';
           } 
           if(depart=="IS" && levl=="level 8" &&(document.getElementById(IS8).style.display == 'none'))
           {
               document.getElementById(IS8).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(IS8).style.display = 'none'; 
           } 
           if(depart=="CS" && levl=="level 1" &&(document.getElementById(CS1).style.display == 'none'))
           {
               document.getElementById(CS1).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(CS1).style.display = 'none';
           } 
           if(depart=="CS" && levl=="level 2" &&(document.getElementById(CS2).style.display == 'none'))
           {
               document.getElementById(CS2).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(CS2).style.display = 'none';
           } 
           if(depart=="CS" && levl=="level 3" &&(document.getElementById(CS3).style.display == 'none'))
           {
               document.getElementById(CS3).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(CS3).style.display = 'none';
           } 
           if(depart=="CS" && levl=="level 4" &&(document.getElementById(CS4).style.display == 'none'))
           {
               document.getElementById(CS4).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(CS4).style.display = 'none';
           } 
           if(depart=="CS" && levl=="level 5" &&(document.getElementById(CS5).style.display == 'none'))
           {
               document.getElementById(CS5).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(CS5).style.display = 'none';

           } 
           if(depart=="CS" && levl=="level 6" &&(document.getElementById(CS6).style.display == 'none'))
           {
               document.getElementById(CS6).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(CS6).style.display = 'none'; 
           } 
           if(depart=="CS" && levl=="level 7" &&(document.getElementById(CS7).style.display == 'none'))
           {
               document.getElementById(CS7).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(CS7).style.display = 'none';
           } 
           if(depart=="CS" && levl=="level 8" &&(document.getElementById(CS8).style.display == 'none'))
           {
               document.getElementById(CS8).style.display = 'inline';
               document.getElementById(pr2).style.display = 'inline';
           }
           else 
           { 
                document.getElementById(CS8).style.display = 'none';
           } 
            if(depart=="your select" || levl=="your select")
           {
               document.getElementById(pr2).style.visibility = "hidden";
           }
           else 
           { 
                document.getElementById(pr2).style.visibility = "visible";
           } 
        }
       
*/
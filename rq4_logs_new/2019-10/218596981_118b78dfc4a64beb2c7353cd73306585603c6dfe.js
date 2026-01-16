

addCustomer.onshow=function(){
    lisName.clear()
    let query = "SELECT name FROM customer"
    req1 = Ajax("https://ormond.creighton.edu/courses/375/ajax-connection.php", "POST", "host=ormond.creighton.edu&user=sch54620&pass=C19971106w&database=sch54620&query=" + query)
    
  if (req1.status == 200) { //transit worked.
    results = JSON.parse(req1.responseText)
    
    //get the array
    if (results.length == 0)
        MSB.MsgBox("There are no company name.")
    else {        
        //console.log("the parsed JSON is " + results)
        //console.log("eg. temp[0] or first row in big array is " + results[0])
        //console.log("to get to Paul, need results[0][1]: " + results[0][1])
        // output the names of all the dogs
        let message = ""
        for (i = 0; i <= results.length - 1; i++){
            message = results[i][0] 
            lisName.addItem(message)
        }
     } 
  } else{
      NSB.MsgBox("Something run")
  }
  hamMenuA.clear()
  hamMenuA.addItem("Sign out") 
  hamMenuA.addItem("See Customer")
  hamMenuA.addItem("Delete Customer")
  hamMenuA.addItem("Edit Customer")
  hamMenuA.addItem("Add Customer")
}

hamMenuA.onclick=function(s){
  if (typeof(s) == "object") { // do nothing
       return
    }
    switch(s) {
      case "Sign Out":
          hmbrMenu.hide()
          btnSignIn.show()
          break
      case "See Customer":
          // do something
          ChangeForm(seeCustomer)
          break
       case "Edit Customer":
          ChangeForm(deleteUpdateCustomer)
          break
       case "Delete Customer":
          ChangeForm(deleteUpdateCustomer)
          break
      case "Add Customer":
          ChangeForm(addCustomer)
          break
     }
}

btnAdd.onclick=function(){
  let newNameA = iptName.value
  let newCityA = iptCity.value
  let newStreetA = iptStreet.value
  let newStateA = iptState.value
  let newZipCodeA = iptZC.value
  let queryInsert = "INSERT INTO customer (name,street,city,state,zipcode) VALUES ('"+newNameA+"', '"+newStreetA+"', '"+newCityA+"','"+ newStateA+"'," +newZipCodeA+")"

    req2 = Ajax("https://ormond.creighton.edu/courses/375/ajax-connection.php", "POST", "host=ormond.creighton.edu&user=sch54620&pass=C19971106w&database=sch54620&query=" + queryInsert)

    if (req2.status == 200) { //transit worked.
        if (req2.responseText == 500) {   // means the insert succeeded
            lisName.clear()
            let message = ""
            let queryNew="SELECT name FROM customer"
            req3 = Ajax("https://ormond.creighton.edu/courses/375/ajax-connection.php", "POST", "host=ormond.creighton.edu&user=sch54620&pass=C19971106w&database=sch54620&query=" + queryNew)
                if (req3.status==200){
                  //transaction works
                  results=JSON.parse(req3.responseText)
                  
                  let message = ""
                  for (i=0; i <= results.length-1; i++){
                      message = results[i][0] 
                      lisName.addItem(message)
                  }
                    
                 let query4 = "SELECT * FROM customer WHERE name=" + '"' + newNameA + '"'
                     
                      req4 = Ajax("https://ormond.creighton.edu/courses/375/ajax-connection.php", "POST", "host=ormond.creighton.edu&user=sch54620&pass=C19971106w&database=sch54620&query=" + query4)
      
                      
                         if (req4.status == 200) { //transit worked.
                               results = JSON.parse(req4.responseText)
                               
                               let message2 = ""
                               for (i = 1; i <= 5; i++)
                                   message2 = message2 + results[0][i] + ", "
                               modInfo.value = message2
                               modInfo.footer= newNameA
                      }      
                      
                      modInfo.toggle()
               }
            
        }else{
            NSB.MsgBox("There was a problem with adding the name to the database.")
        }
    } else {
        // transit error
        NSB.MsgBox("Error: " + req1.status)
    }  
}


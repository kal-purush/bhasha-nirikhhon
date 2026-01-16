let db = window.localStorage;


function loadAllTicket()
{
   let allTickets = db.getItem("allTickets");

   if(allTickets)
  {
      allTickets = JSON.parse(allTickets);
 
      for(let i=0;i<allTickets.length;i++)
      {
          let tikinfo = allTickets[i];
          addTikt(tikinfo);
      }
      
  }
  
}

function saveTicketToDb(tikinfo)
{
  let allTickets = db.getItem("allTickets");

  if(allTickets)
  { 
      allTickets = JSON.parse(allTickets);
      allTickets.push(tikinfo);
      db.setItem("allTickets",JSON.stringify(allTickets));      
  }
  else
  {
     let allTickets = [tikinfo];
     db.setItem("allTickets",JSON.stringify(allTickets));
  }
   
}

function deleteTicketfromDb(ticketId)
{
   let allTickets = JSON.parse(db.getItem("allTickets"));
   
   let updatedticket = allTickets.filter( function(tikt){
     if(tikt.tiktID==ticketId)
{  
   return false;
}
else{
    return true;
}
});

db.setItem("allTickets",JSON.stringify(updatedticket));
   
}
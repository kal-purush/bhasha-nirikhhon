let fetchOps = {
  url: 'url'
}

fetch(fetchOps)
  .then(function(res){
    if(res.status < 200 || res.status >= 300){
      console.log("Status: ", res.status);
    }
    // Main function
  })
  .catch(function(err){
    console.log("Error: ", err);
  })
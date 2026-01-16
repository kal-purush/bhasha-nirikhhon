/* globals fetch */
var update = document.getElementById('update')
var del = document.getElementById('delete')

// update.addEventListener('click', function () {
//   fetch('quotes', {
//     method: 'put',
//     headers: {'Content-Type': 'application/json'},
//     body: JSON.stringify({
//       'name': 'Darth Vader',
//       'quote': 'I find your lack of faith disturbing.'
//     })
//   })
//   .then(response => {
//     if (response.ok) return response.json()
//   })
//   .then(data => {
//     console.log(data)
//   })
// })

// del.addEventListener('click', function () {
//   fetch('quotes', {
//     method: 'delete',
//     headers: {
//       'Content-Type': 'application/json'
//     },
//     body: JSON.stringify({
//       'name': 'Darth Vader'
//     })
//   }).then(function (response) {
//     window.location.reload()
//   })
// })

function upVote(id,votes,data)
{
  let postId=id;
  let upVotes=votes;
  fetch('upvote',{
    method:'post',
     headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      'postId':postId,
      'upVotes':upVotes,
      'postData':data
    })
  }).then(function(response)
  {
  let currentVotes=  document.getElementById(id).childNodes[3].innerHTML;
  console.log(currentVotes);
   var postTag=document.getElementById(id).childNodes[3].innerHTML=parseInt(currentVotes)+1;
   console.log(postTag);
  })
}
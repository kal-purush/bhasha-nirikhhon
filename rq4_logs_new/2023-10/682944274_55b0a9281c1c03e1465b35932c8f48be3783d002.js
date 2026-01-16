
  const ably = new Ably.Realtime({
  key: 'eONy9A.WwyHFA:oGIzqxK8nbVGngoAuwBCNVuvCzo8uNniiPBxea7NLtI',
  clientId:'user__'+loginUser,

  }
);



ably.connect();
  const channel=ably.channels.get('online-users');

  channel.presence.enter();
 
// ably.auth.requestToken({ clientId: 'randeep'}, function(err, tokenDetails){
//   if(err) {
//     console.log('An error occurred; err = ' + err.message);
//   } else {
//     console.log('Success; token = ' + tokenDetails.token);
//   }
// });



channel.subscribe('user-'+loginUser+'-message', function(message) {
  //console.log(message);
   if(messanger.messageContainer)
      {
        messanger.receiveMessage(message);
      }
});

function sendAblyMessage(user_id,messageData)
{
  channel.publish('user-'+user_id+'-message', messageData, (err) => {
  if (err) {
    console.error('Error publishing message:', err);
  } else {
     
  }
});
}

channel.presence.subscribe('enter', (member) => {
   let user_id=member.clientId.replace('user__','');

        if(document.querySelector('.userStatus__'+user_id))  


      document.querySelector('.userStatus__'+user_id).classList.add('bg-success');
});

channel.presence.subscribe('leave', (member) => {
        let user_id=member.clientId.replace('user__','');

        if(document.querySelector('.userStatus__'+user_id))

      document.querySelector('.userStatus__'+user_id).classList.remove('bg-success');

});


function checkUserStatus()
{
channel.presence.get(function(err, members) {
    if(members.length)
    {
     members.forEach((member, index) => {
      let user_id=member.clientId.replace('user__','');
  if(document.querySelector('.userStatus__'+user_id))
  {
      document.querySelector('.userStatus__'+user_id).classList.add('bg-success');
  }
});
    }
});
}

checkUserStatus();

const ablyApiKey = 'eONy9A.WwyHFA:oGIzqxK8nbVGngoAuwBCNVuvCzo8uNniiPBxea7NLtI';
const ablyEndpoint = 'https://rest.ably.io/channels';
const resultArea ='';

//request a list of channels on button click
function enumerateChannels() {
  const endpoint = '/channels';
  ably.request('get', endpoint, { limit: 100, by: 'id' }, null, null, handleResultPage);
}

let channelCount = 0;
function handleResultPage(err, resultPage) {
  if(err || !resultPage.success) {
    resultArea.value += 'An error occurred; err = ' + (err || resultPage.errorMessage);
    return;
  }
  if(channelCount === 0) {
    if(resultPage.items.length == 0){
      resultArea.value += "Your app does not have any active channels\n";
      return;
    }
    resultArea.value += "Your app has the following active channels:\n";
  }

   console.log(resultPage);


  resultPage.items.forEach(function(channel) {
  })

  if(resultPage.hasNext()) {
    resultPage.next(handleResultPage);
  };
}

//enumerateChannels();

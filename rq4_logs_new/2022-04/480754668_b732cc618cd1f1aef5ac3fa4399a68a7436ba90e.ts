// TEST ONLY
process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = '0';
// TEST ONLY

const axios = require('axios');
require('dotenv').config();


interface Card {
  id: string;
  due: string;
}


const config = {
  method: 'get',
  url: `https://api.trello.com/1/lists/${process.env.LIST_ID}?key=${process.env.API_KEY}&token=${process.env.API_TOKEN}&cards=open&fields=cards`,
};

axios(config)
.then(function (response: any) {
  const cards: Card[] = response.data.cards.filter((card: any) => card.due !== null).map((card: any) => { 
    return {
      id: card.id,
      due: card.due
    }
  }).filter((card: any) => 
    new Date(card.due) < new Date(new Date().toDateString())
  );

  cards.forEach((card: Card) => {
    const updateConfig = {
      method: 'put',
      url: `https://api.trello.com/1/cards/${card.id}?key=${process.env.API_KEY}&token=${process.env.API_TOKEN}`,
      headers: { 
        'Content-Type': 'application/json', 
      },
      data : {
        due: new Date().toISOString().split('T')[0] + 'T21:59:59Z'
      }
    };
    
    axios(updateConfig)
    .then(function (updateResponse: any) {
      console.log(JSON.stringify(updateResponse.data))
      console.log("Set due date to today for card: " + card.id);
    })
    .catch(function (updateError: any) {
      console.log(updateError);
    });
  })
})
.catch(function (error: any) {
  console.log(error);
});
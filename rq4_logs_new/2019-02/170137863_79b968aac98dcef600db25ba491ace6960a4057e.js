const shoes = [
  {
    id: 0,
    brand: 'Allen Edmonds',
    model: '5th Ave',
    size: 8,
    condition: 'Worn 5 times',
    price: 100,
    bids: 0,
    highBidder: '',
    pin: 1111
  },
  {
    id: 1,
    brand: 'Allen Edmonds',
    model: '5th Ave',
    size: 10.5,
    condition: 'Worn 5 times',
    price: 100,
    highBidder: '',
    bids: 0,
    pin: 1111
  }
  
]


let id = 2


module.exports = {
  getShoes: (req, res) => {
    res.status(200).send(shoes)
  },

  createShoe: (req, res) => {
    const {brand, model, condition, price, pin} = req.body
    shoes.push({
      id,
      brand,
      model,
      condition,
      price,
      bids: 0,
      pin
    })
    id++
    res.status(200).send(shoes)
  },

  deleteShoes: (req, res) => {
    const {id} = req.params

    const index = shoes.findIndex(shoes => shoes.id == id)

    shoes.splice(index, 1)

  res.status(200).send(shoes)
  console.log('hit')

  },

  increaseBid: (req, res) => {
    const {id} = req.params
    // const {price, bids, highBidder} = req.body
    console.log('-------------------------------')
    console.log(req.body)
    
    const index = shoes.findIndex(shoes => shoes.id == id)
    shoes[index].price = +shoes[index].price +5
    shoes[index].bids += 1
    shoes[index].highBidder = req.body.highBidder
    res.status(200).send(shoes)
  },

  updateCondition: (req, res) => {
    const {id} = req.params
    const {condition} = req.body

    const index = shoes.findIndex(shoes => shoes.id == id)

    shoes[index].condition = condition
    res.status(200).send(shoes)
  }
 
  }
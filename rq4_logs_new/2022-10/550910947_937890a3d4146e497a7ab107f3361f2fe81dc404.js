// counter 0,1,2 number
// fontSize 16   number
// backgroundColor string

let state = {
    counter: 0,
    fontSize: 16,
    backgroundColor: 'red'
}

// working yay
// this function has a side effect (mutation)
function increment(){  
  state.counter++
}

increment()

console.log(state)

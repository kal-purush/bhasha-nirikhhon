var priceSum = items.reduce(function(a,b){
    return a + b.price
}, 0)

var avg = Math.round((priceSum / items.length)*100)/100

document.querySelector("#answer1").innerHTML = `The average price for all items is $${avg}\n`





var newArray = items.filter(function(a){
    return (a.price > 14 && a.price < 18)
})

var lowPriceItems = newArray.map (function(a){
    return a.title
})


document.querySelector("#answer2").innerHTML = `${lowPriceItems.join("\n")}\n`





var gbpItem = items.filter(function(a){
    return a.currency_code == "GBP"
})[0]

var gbpItemTitle = gbpItem.title

document.querySelector("#answer3").innerHTML = `${gbpItem.title} costs $${gbpItem.price}\n`




var includeWood = items.filter(function(a){
    return a.materials.includes("wood")
})

var includeWoodItems = includeWood.map(function(a){
    return a.title + " is made of wood."
})

document.querySelector("#answer4").innerHTML = `${includeWoodItems.join("\n\n")}\n`



var eightOrMore = items.filter(function(a){
    return a.materials.length >= 8
})

var eightOrMoreAttributes = eightOrMore.map(function(a){
    return a.title + " has " + a.materials.length + " materials:\n\n" + a.materials.join("\n") + "\n\n"
})


document.querySelector("#answer5").innerHTML = eightOrMoreAttributes.join("")



var madeBySellers = items.filter(function(a){
    return a.who_made=="i_did"
})

document.querySelector("#answer6").innerHTML = `${madeBySellers.length} were made by their sellers`
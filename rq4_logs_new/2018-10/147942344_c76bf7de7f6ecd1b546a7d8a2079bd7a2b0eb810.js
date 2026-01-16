const BREWERIES = {}

BREWERIES.show = () => {
  $("#breweriestable tr:gt(0)").remove()
  const table = $("#brewerytable")

  BREWERIES.list.forEach((brewery) =>{
    let future = '<tr>'
    + '<td>' + brewery['name'] + '</td>'
    + '<td>' + brewery['year'] + '</td>'
    + '<td>' + brewery['beers'] + '</td>'

    if (brewery['active']) {
      future +=  '<td>Yes</td>'
    }else {
      future +=  '<td>No</td>'
    }
    future += '</tr>'

    table.append(future)
  })
}

document.addEventListener("turbolinks:load", () => {
  console.log("moiu")
  if ($("#brewerytable").length == 0) {
    return
  }

  $("#name").click((e) =>{
    e.preventDefault()

  })


  $.getJSON('breweries.json', (breweries) => {
    panimot = breweries
    BREWERIES.list = breweries
    BREWERIES.show()
  })
})
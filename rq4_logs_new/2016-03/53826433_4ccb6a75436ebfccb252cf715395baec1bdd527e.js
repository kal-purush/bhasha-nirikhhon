console.log(Backbone) 
console.log("sketti time")//<---test to see if js is connected correctly.

//api request url syntax

//https://openapi.etsy.com/v2/listings/active.js?api_key=**KEYHERE**&callback=?

//api key: "4qbvpiz0vok42wn8xewnojn5"

//--------Models----------------//

// extend the native Backbone with our etsy model constructor. this constructor adds a custom api key value, and assigns the api request url to the url attribute.

var EtsyHomeModel = Backbone.Model.extend ({

	_apiKey: "4qbvpiz0vok42wn8xewnojn5",
	url: "https://openapi.etsy.com/v2/listings/active.js?"

})

var EtsyDetailModel = Backbone.Model.extend ({
_apiKey: "4qbvpiz0vok42wn8xewnojn5",
url: "https://openapi.etsy.com/v2/listings/active.js?"
})
//need to add a detail Model here.

//----Views---------------//

//extending the native backbone view with our EtsyHomeView constructor. Assigning our container element to the "el" attribute.

var EtsyHomeView = Backbone.View.extend({

	el: "#container",

	initialize: function(someModel) {
	this.model = someModel	
	console.log(this.model)
	var newFunc = this._render.bind(this)
	this.model.on("sync", newFunc)	
	},
	events: {
		"click img": "_triggerDetailView"
			},

	_triggerDetailView: function(clickEvent) {
		var imageNode = clickEvent.target
		location.hash = "detail/" + imageNode.getAttribute("listingid")
		console.log(clickEvent.target)
	},		
			
	_render: function(){
		var dataArray = this.model.get("results")
		console.log(this.model)
		console.log(dataArray)
		var itemUrlString = ""
		console.log(dataArray[0])
	for (var i = 0; i < 12; i++) {
		var itemObj = dataArray[i]
		console.log(itemObj.Images[0].url_570xN)
		var itemId = itemObj.listing_id
	console.log(itemObj.listing_id)	
	itemUrlString += '<img class="etsyHome" listingId="' + itemId + '"src="' + itemObj.Images[0].url_570xN + '">'
	}	
		this.el.innerHTML = itemUrlString

	console.log(dataArray)
	//need to add for loop to get each item, and this.el.innterHTML = itemUrlString
	}

})

var EtsyDetailView = Backbone.View.extend ({
	el: "#container",

	initialize: function(someModel) {
		this.model = someModel
		var newFunc = this._render.bind(this)
		this.model.on("sync", newFunc)
	},

_render: function() {

	console.log(this.model.attributes.results)
	var itemUrl = this.model.attributes.results.Images.url_570xN

	this.el.innerHTML = '<img class="etsyDetail" src="' + itemUrl + '"></img>'
}	
})

var EtsyRouter = Backbone.Router.extend ({

	routes: {
		"home/": "handleHomeView",
		"detail/:id": "handleDetailView"
	},

	handleHomeView: function(){
		var mod = new EtsyHomeModel()
		var vew = new EtsyHomeView(mod)

		mod.fetch({
			contentType: "application/json",
			dataType: "jsonp",
			data:{
				includes:"Images",
				api_key: mod._apiKey,
				// callback:"?"
			}
		})
	},
	handleDetailView: function(itemId){
		var singleModel = new EtsyDetailModel()
		var detailView = new EtsyDetailView(singleModel)
		singleModel.url += itemId
		singleModel.fetch({
			contentType: "application/json",
			dataType: "jsonp",
			data:{
				includes:"Images",
				api_key: singleModel._apiKey
				// callback: "?"
			}
		})

	},
	initialize: function() {
		Backbone.history.start()
	}
})

var rtr = new EtsyRouter()
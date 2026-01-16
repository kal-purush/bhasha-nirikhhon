var shares = 1;
var invested = 1;
var holdings = $("#holdings")[0];
var value = $("#value")[0];
var initial = $("#initial")[0];
var gain = $("#gain")[0];

holdings.innerText = shares.toLocaleString();
initial.innerText = "$" + invested;

checkPrice();

function checkPrice() {
	$.ajax({
		url: "https://api.dex.guru/v1/tokens/0x7e5d52c3335c91af0da392bea4bb9e43f2aba62c-bsc/",
		dataType: "json",
		timeout: 30000,
		success: function(data) {
			$("#price").html("$" + data.priceUSD.toFixed(10));
			value.innerText = "$" + (data.priceUSD * shares).toFixed(2);
			gain.innerText = (((data.priceUSD * shares) / invested) * 100).toFixed(2) + "%";
			checkPrice();
		},
		error: function() {
			checkPrice();
		}
	});
}
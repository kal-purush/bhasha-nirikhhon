const baseUrl = "https://checkout.citybeauty.com";
const apiUrl = "https://dbh99ppw9f.execute-api.us-east-1.amazonaws.com/prod/api";
var nextpage = "";

function getCheckoutNameInURL(checkoutcode) {
  const path = window.location.pathname;
  const pagename = path.split("/").pop();
  return pagename.split(".html")[0];
}

function getParameterByName(name, url) {
    if (!url) url = window.location.href;
    name = name.replace(/[\[\]]/g, "\\$&");
    var regex = new RegExp("[?&]" + name + "(=([^&#]*)|&|#|$)"),
    results = regex.exec(url);
    if (!results) return null;
    if (!results[2]) return '';
    return decodeURIComponent(results[2].replace(/\+/g, " "));
}

var ccToken = getParameterByName('token');
var checkoutID = getParameterByName('checkoutid');
var chtx = parseFloat(getParameterByName('chtx'));
var pid = getParameterByName('pid');
var checkouts;
var shippingRate;
var amountValue;
var productValue;
$.get(`${apiUrl}/getFunnel`, successGetFN);
function successGetFN(data, status) {
  if (status == 'success') {
    pagename = getCheckoutNameInURL();
    num = parseInt(pid.slice(-1)) - 1;
    offers = data.checkouts[num].funnels[0].offers;
    offers.map(function (item) {
      if (pagename == item.pagename) {
        $('#product_name').html(item.title);
        $('#product_price').html(item.product.price);
        nextpage = item.nextpage;
      }
    });
    shippingRate = data.shipRate;
    if (chtx > 0) {
      amountValue = parseFloat(checkouts.funnels[0].offers[0].product.price) * chtx;
      amountValue = amountValue.toFixed(2);
    }else {
      amountValue = parseFloat(checkouts.funnels[0].offers[0].product.price);
    }
    productValue = checkouts.funnels[0].offers[0].product;
    return;
  }
  else {
    alert("We're having issue with network! Please try again!!");
    return;
  }
}

$('#submit').click(function (event) {
  event.preventDefault();
  var formdata = {};
  formdata.amount = amountValue;
  formdata.token = ccToken;
  formdata.product = productValue;
  formdata.checkoutID = checkoutID;
  formdata.chtx = chtx;
  $.ajax({
      type: 'POST',
      data: JSON.stringify(formdata),
      contentType: 'application/json',
      crossDomain: true,
      url: `${apiUrl}/aClickCharge`,
      success: function(data, status){
        if (nextpage == "orderconfirmation") {
          window.location = `https://citybeauty.com/orderconfirmation.php?checkoutid=${checkoutID}`;
        }
        else {
          window.location = `${baseUrl}/src/fnl/${nextpage}.html?pid=${pid}&token=${data.transaction.creditCard.token}&checkoutid=${checkoutID}&chtx=${chargeTax}`;
        }
      },
      error: function (data, status) {
        console.log(status);
        return;
      }
  })
});
$('#NotTakeOffer').click(function (event) {
  event.preventDefault();
  window.location = `https://citybeauty.com/orderconfirmation.php?checkoutid=${checkoutID}`;
});
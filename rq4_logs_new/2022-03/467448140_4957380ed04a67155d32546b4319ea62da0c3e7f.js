const dotenv = require('dotenv');
const e = require('express');
dotenv.config();
var sandboxCredentials  = {
    username : "AdtQFOeXF-CnTJKGOuTNZ_WNJSOA1qL2TKoyuTkouG2M2qmec3zlckAUDcG-rdIEL4Wf6qhQ_x_4QiSY",
    password : "EIzdGzYGHN_Uy7bGrcry9Xouc_xz4RCP15k-G_N-GLsm-5y83-dEN0lPQFeVfP5IM9NuQXOQUBLEtiVf"
}
var pordCredentials ={
    username : "Aad1o14bnhMZqIoODWXDItN5V0VfhNd3_mpsUh0tg-mJZ-QeB6DayzWEX6cLsldXMKAzAmO3__IEJOwl",
    password : "ENqMueJPYKfZ5pklICh6mT5ysC0y27LVXBOuZcYWYJzObgzKlaURLZUItHEKPix4prxo1EpmsPQkDncY"
}
var sbUrl = {
    auth:"https://api.sandbox.paypal.com/v1/oauth2/token",
    quoteUrl : "https://api.sandbox.paypal.com/v2/pricing/quote-exchange-rates",
    exchangeUrl : "https://api.sandbox.paypal.com/v2/pricing/exchange-currency"
}
var prodUrl = {
    auth : "https://api.paypal.com/v1/oauth2/token",
    quoteUrl : "https://api.paypal.com/v2/pricing/quote-exchange-rates",
    exchangeUrl : "https://api.paypal.com/v2/pricing/exchange-currency"
}

var sbaccesstoken = [];
var prodaccesstoken = [];

module.exports = function Environment(){

 this.setAccessToken = function(access_token,mode){
    if(mode == "sandbox"){
        if(sbaccesstoken.length >=1){
            sbaccesstoken.pop();
        }
        sbaccesstoken.push(access_token);
    }
    else{
        if(prodaccesstoken.length >=1){
            prodaccesstoken.pop();
        }
        console.log("pushing access token to list");
        prodaccesstoken.push(access_token);
    }
    return true;
 }
 this.getAccessToken = function(mode){
    if(mode == 'sandbox'){
        return sbaccesstoken[0];
    }
    else{
    return prodaccesstoken[0];
    }
 }
 this.getQuoteURL = function(mode){
    if(mode =='sandbox'){
        return sbUrl.quoteUrl;
    }
    else{
         return prodUrl.quoteUrl;
    }
 }
 this.getAuthURL = function(mode){
    if(mode =='sandbox'){
        return sbUrl.auth;
    }
    else{
         return prodUrl.auth;
    }
 }
 this.getusername = function(mode){
    if(mode =='sandbox'){
        return sandboxCredentials.username;
    }
    else{
         return pordCredentials.username;
    }
 }
 this.getpassword = function(mode){
    if(mode =='sandbox'){
        return sandboxCredentials.password;
    }
    else{
         return pordCredentials.password;
    }
 }
}
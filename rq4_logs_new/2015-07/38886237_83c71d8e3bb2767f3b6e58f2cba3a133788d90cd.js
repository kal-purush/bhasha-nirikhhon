var dbContex = require('../data/DBContext.js');
var orderModel = require('../models/order.js');
var siteModel = require('../models/site.js');

exports.get=function(lat,long){
	
	var order= new orderModel.Order();
	order._id= "dfdsfsdfsdf";
	order.MenuItems= null;
	order.distance =2.5;
	
	var site1= new siteModel.Site();
	site1.Name="John Name";
	site1.SiteId="1";
	site1.Address.Address1='363 Bridgestone Ln';
	site1.Address.City='Atlanta';
	site1.Address.State='GA';
	site1.Address.Zip='30134';
	site1.Address.longitude=long;
	site1.Address.latitude= lat;
	order.site= site1;
	
	return [order];
};
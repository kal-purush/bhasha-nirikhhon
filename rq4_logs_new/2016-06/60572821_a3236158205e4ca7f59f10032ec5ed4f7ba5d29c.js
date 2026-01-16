/* jshint node:true,esnext:true,eqeqeq:true,undef:true,lastsemic:true,strict:true */
module.exports = function(aoa, header, start){
    'use strict';
    var h,s,i,l,row,j,k;
    var result = {};
    if (!(Array.isArray(aoa))) return;
    if (aoa.length<1) return;
    if (!(Array.isArray(aoa[0]))) return;
    if (typeof(header)==='object') {
	s = start || 0;
	if (Array.isArray(header.pluck)){
	    s = start || 1;
	    h = {};
	    for(i=0,l=header.pluck.length;i<l;++i){
		j = aoa[0].indexOf(header.pluck[i]);
		if (j>=0)
		    h[j] = aoa[0][j];
	    }
	} else {
	    h = header;
	}
    } else if (typeof(header)==='undefined'){
	h = aoa[0];
	s = 1;
    } else {
	return;
    }
    if (Array.isArray(h)){
	for(i=0,l=h.length;i<l;++i)
	    if (h[i])
		result[h[i]] = [];
    } else {
	for(i in h)
	    if (h.hasOwnProperty(i))
		result[h[i]] = [];
    }
    for(i=s,l=aoa.length;i<l;++i){
	row = aoa[i];
	for(j=0,k=row.length;j<k;++j)
	    if (h[j])
		result[h[j]][i-s] = row[j];
    }
    return result;
};
    
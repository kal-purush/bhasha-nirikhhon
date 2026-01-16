
//===============
// object2String
//===============
function object2String(thisObject) {
//console.log("object="+thisObject);
	if (thisObject == null)
		return "null";
	if (typeof(thisObject)=='undefined')
		return "Undefined";
//console.log("prototype="+thisObject.prototype);
	  
	switch (typeof(thisObject)) {
		case 'string':  return "'"+thisObject+"'";
		case 'number':  return thisObject.toString();
		case 'object':
//console.log("-->thisObject="+thisObject.constructor.getOwnPropertyNames+" = "+Object.getOwnPropertyNames(thisObject.constructor));
			var listOfObjects = Object.getOwnPropertyNames(thisObject).toString().split(',');
			var returnValue = "";
			if (thisObject instanceof Array)
				returnValue += "[";   else   returnValue += "{";
			for (var i=0;i<listOfObjects.length;i++) {
				returnValue += listOfObjects[i]+"="+object2String(thisObject[listOfObjects[i]]);
				if (i<(listOfObjects.length-1)) {
					returnValue += ", ";
				}
			}
			if (thisObject instanceof Array)
				returnValue += "]";   else   returnValue += "}";
			return returnValue;
		default: return " unknown("+typeof(thisObject)+")";
	}
}

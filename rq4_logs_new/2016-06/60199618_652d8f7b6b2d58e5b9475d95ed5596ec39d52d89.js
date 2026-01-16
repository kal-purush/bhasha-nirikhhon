/*!
 * stuQuery version 1.0.1
 */
var eventcache={};function S(h){function f(m,e){var s=new Array();var q,r,p,n,l,o;if(e.indexOf(":eq")>=0){q=e.split(" ");for(p=0;p<q.length;p++){if(p==0){o=c(m,q[p])}else{r=new Array();for(n=0;n<o.length;n++){r=r.concat(c(o[n],q[p]))}o=r.splice(0)}}}else{o=m.querySelectorAll(e)}for(l=0;l<o.length;l++){s.push(o[l])}return s}function c(p,o){var n=-1;var l=new Array();if(o.indexOf(":eq")>0){var j=o.replace(/(.*)\:eq\(([0-9]+)\)/,"$1 $2").split(" ");o=j[0];n=parseInt(j[1])}if(o[0]=="."){els=p.getElementsByClassName(o.substr(1))}else{if(o[0]=="#"){els=p.getElementById(o.substr(1))}else{els=p.getElementsByTagName(o)}}if(!els){els=[]}if(els.nodeName&&els.nodeName=="SELECT"){l.push(els)}else{if(typeof els.length!=="number"){els=[els]}for(k=0;k<els.length;k++){l.push(els[k])}if(n>=0&&l.length>0){if(n<l.length){l=[l[n]]}else{l=[]}}}return l}function a(n,m){var j=false;if(m[0]=="."){m=m.substr(1);for(var l=0;l<n.classList.length;l++){if(n.classList[l]==m){return true}}}else{if(m[0]=="#"){if(n.id==m.substr(1)){return true}}else{if(n.tagName==m.toUpperCase()){return true}}}return false}function d(e){var j;if(typeof e==="string"){this.e=f(document,e)}else{if(typeof e==="object"){this.e=(typeof e.length=="number")?e:[e]}}this.length=(this.e?this.e.length:0);return this}d.prototype.ready=function(e){/in/.test(document.readyState)?setTimeout("S(document).ready("+e+")",9):e()};d.prototype.html=function(j){if(typeof j==="number"){j=""+j}if(typeof j!=="string"&&this.e.length==1){return this.e[0].innerHTML}if(typeof j==="string"){for(var e=0;e<this.e.length;e++){this.e[e].innerHTML=j}}return this};d.prototype.append=function(j){if(!j&&this.e.length==1){return this.e[0].innerHTML}if(j){for(var e=0;e<this.e.length;e++){this.e[e].innerHTML+=j}}return this};d.prototype.prepend=function(l){if(!l&&this.e.length==1){return this.e[0].innerHTML}if(l){for(var m=0;m<this.e.length;m++){this.e[m].innerHTML=l+this.e[m].innerHTML}}return this};d.prototype.before=function(n){var p=document.createElement("div");p.innerHTML=n;var o=p.childNodes;for(var m=0;m<el.length;m++){for(var l=0;l<o.length;l++){el[m].parentNode.insertBefore(o[l],el[m])}}return this};d.prototype.after=function(j){for(var e=0;e<this.e.length;e++){this.e[e].insertAdjacentHTML("afterend",j)}return this};function i(e,l){if(e&&e.length>0){for(var j=0;j<e.length;j++){if(e[j].node==l){return{success:true,match:j}}}}return{success:false}}function g(o,m,l,j,n){if(!eventcache[m]){eventcache[m]=new Array()}eventcache[m].push({node:o,fn:l,fn2:j,data:n})}function b(l){if(eventcache[l.type]){var j=i(eventcache[l.type],l.currentTarget);if(j.success){if(j.match.data){l.data=eventcache[l.type][j.match].data}return{fn:eventcache[l.type][j.match].fn,data:l}}}return function(){return{fn:""}}}d.prototype.off=function(l){if(typeof Element.prototype.removeEventListener!=="function"){Element.prototype.removeEventListener=function(s,p){if(!oListeners.hasOwnProperty(s)){return}var o=oListeners[s];for(var m=-1,n=0;n<o.aEls.length;n++){if(o.aEls[n]===this){m=n;break}}if(m===-1){return}for(var r=0,q=o.aEvts[m];r<q.length;r++){if(q[r]===p){q.splice(r,1)}}}}for(var j=0;j<this.e.length;j++){var e=i(eventcache[l],this.e[j]);if(e.success){this.e[j].removeEventListener(l,eventcache[l][e.match].fn2,false);eventcache[l].splice(e.match,1)}}return this};d.prototype.on=function(m,n,l){m=m||window.event;this.cache=[4,5,6];if(typeof n==="function"&&!l){l=n;n=""}if(typeof l!=="function"){return this}if(this.e.length>0){var o=this;var e=function(p){var q=b({currentTarget:this,type:m,data:n,originalEvent:p});if(typeof q.fn==="function"){return q.fn.call(o,q.data)}};for(var j=0;j<this.e.length;j++){g(this.e[j],m,l,e,n);if(this.e[j].addEventListener){this.e[j].addEventListener(m,e,false)}else{if(this.e[j].attachEvent){this.e[j].attachEvent(m,e)}}}}return this};d.prototype.trigger=function(m){var l;if(document.createEvent){l=document.createEvent("HTMLEvents");l.initEvent(m,true,true)}else{l=document.createEventObject();l.eventType=m}l.eventName=m;for(var j=0;j<this.e.length;j++){if(document.createEvent){this.e[j].dispatchEvent(l)}else{this.e[j].fireEvent("on"+l.eventType,l)}}return this};d.prototype.focus=function(){if(this.e.length==1){this.e[0].focus()}return this};d.prototype.blur=function(){if(this.e.length==1){this.e[0].blur()}return this};d.prototype.remove=function(){if(!this.e){return this}for(var e=this.e.length-1;e>=0;e--){if(!this.e[e]){return}if(typeof this.e[e].remove==="function"){this.e[e].remove()}else{if(typeof this.e[e].parentElement.removeChild==="function"){this.e[e].parentElement.removeChild(this.e[e])}}}return S(this.e)};d.prototype.hasClass=function(j){var e=true;for(var l=0;l<this.e.length;l++){if(!this.e[l].className.match(new RegExp("(\\s|^)"+j+"(\\s|$)"))){e=false}}return e};d.prototype.toggleClass=function(e){for(var j=0;j<this.e.length;j++){if(this.e[j].className.match(new RegExp("(\\s|^)"+e+"(\\s|$)"))){this.e[j].className=this.e[j].className.replace(new RegExp("(\\s|^)"+e+"(\\s|$)","g")," ").replace(/ $/,"")}else{this.e[j].className=(this.e[j].className+" "+e).replace(/^ /,"")}}return S(this.e)};d.prototype.addClass=function(e){for(var j=0;j<this.e.length;j++){if(!this.e[j].className.match(new RegExp("(\\s|^)"+e+"(\\s|$)"))){this.e[j].className=(this.e[j].className+" "+e).replace(/^ /,"")}}return S(this.e)};d.prototype.removeClass=function(e){for(var j=0;j<this.e.length;j++){while(this.e[j].className.match(new RegExp("(\\s|^)"+e+"(\\s|$)"))){this.e[j].className=this.e[j].className.replace(new RegExp("(\\s|^)"+e+"(\\s|$)","g")," ").replace(/ $/,"").replace(/^ /,"")}}return S(this.e)};d.prototype.css=function(m){var o;for(var l=0;l<this.e.length;l++){o={};var n=this.e[l].getAttribute("style");if(n){var q=this.e[l].getAttribute("style").split(";");for(var j=0;j<q.length;j++){var p=q[j].split(":");if(p.length==2){o[p[0]]=p[1]}}}if(typeof m==="object"){for(key in m){o[key]=m[key]}var e="";for(key in o){if(e){e+=";"}if(o[key]){e+=key+":"+o[key]}}this.e[l].setAttribute("style",e)}}if(this.e.length==1&&typeof m==="string"){return o[m]}return S(this.e)};d.prototype.parent=function(){var j=[];for(var e=0;e<this.e.length;e++){j.push(this.e[e].parentElement)}return S(j)};d.prototype.children=function(m){if(typeof m==="string"){var e=[];for(var j=0;j<this.e.length;j++){for(var l=0;l<this.e[j].children.length;l++){if(a(this.e[j].children[l],m)){e.push(this.e[j].children[l])}}}return S(e)}else{for(var j=0;j<this.e.length;j++){this.e[j]=(this.e[j].children.length>m?this.e[j].children[m]:this.e[j])}return S(this.e)}};d.prototype.find=function(j){var m=[];var e=new Array();for(var l=0;l<this.e.length;l++){e=e.concat(f(this.e[l],j))}return S(e)};d.prototype.attr=function(e,m){var l=[];for(var j=0;j<this.e.length;j++){l.push(this.e[j].getAttribute(e));if(typeof m==="string"||typeof m==="number"){if(m){this.e[j].setAttribute(e,m)}else{this.e[j].removeAttribute(e)}}}if(l.length==1){l=l[0]}if(typeof m==="undefined"){return l}else{return S(this.e)}};d.prototype.prop=function(e,m){var l=[];for(var j=0;j<this.e.length;j++){l.push(this.e[j].getAttribute(e));if(typeof m==="boolean"){if(m){this.e[j].setAttribute(e,e)}else{this.e[j].removeAttribute(e)}}}if(l.length==1){l=l[0]}return l};d.prototype.clone=function(){var e=document.createElement("div");e.appendChild(this.e[0].cloneNode(true));return e.innerHTML};d.prototype.replaceWith=function(j){var l=document.createElement("span");l.innerHTML=j;var m=S(this.e);for(var e=0;e<this.e.length;e++){m.e[0].parentNode.replaceChild(l,m.e[0])}return m};d.prototype.ajax=function(m,l){if(typeof m!=="string"){return false}if(!l){l={}}l.url=m+(typeof l.cache==="boolean" && !l.cache ? '?'+(new Date()).valueOf():'');var o=(window.XMLHttpRequest)?new XMLHttpRequest():new ActiveXObject("Microsoft.XMLHTTP");o.addEventListener("load",e);o.addEventListener("error",j);function e(p){if(o.status===200){l.header=o.getAllResponseHeaders();if(typeof l.complete==="function"){l.complete.call((l["this"]?l["this"]:this),(l.dataType=="json")?JSON.parse(o.responseText):o.responseText,l)}}else{j(p)}}function j(p){if(typeof l.error==="function"){l.error.call((l["this"]?l["this"]:this),p,l)}}try{o.open("GET",m)}catch(n){j(n)}try{o.send()}catch(n){j(n)}return this};d.prototype.loadJSON=function(j,l,e){if(!e){e={}}e.dataType="json";e.complete=l;this.ajax(j,e);return this};return new d(h)};

function CSV2JSON(data,format,start,end){

	if(typeof start!=="number") start = 1;
	if(typeof data==="string") data = data.split(/[\n\r]/);
	if(typeof end!=="number") end = data.length;
	var line,datum;
	var newdata = new Array();

	for(var i = start; i < end; i++){
		line = data[i].split(/,/);
		datum = {};
		for(var j=0; j < line.length; j++){
			if(format[j]){
				if(format[j].format=="number"){ if(line[j]!=""){ if(line[j]=="infinity" || line[j]=="Inf"){ datum[format[j].name] = Number.POSITIVE_INFINITY; }else{ datum[format[j].name] = parseFloat(line[j]); }}}
				else if(format[j].format=="eval"){ if(line[j]!="") datum[format[j].name] = eval(line[j]); }
				else if(format[j].format=="date"){ datum[format[j].name] = new Date(line[j].replace(/^"/,"").replace(/"$/,"")); }
				else if(format[j].format=="boolean"){ if(line[j]=="1" || line[j]=="true"){ datum[format[j].name] = true; }else if(line[j]=="0" || line[j]=="false"){ datum[format[j].name] = false; }else{ datum[format[j].name] = null;} }
				else{ datum[format[j].name] = (line[j][0]=='"' && line[j][line[j].length-1]=='"') ? line[j].substring(1,line[j].length-1) : line[j]; }
			}else{
				datum[j] = (line[j][0]=='"' && line[j][line[j].length-1]=='"') ? line[j].substring(1,line[j].length-1) : line[j];
			}
		}
		newdata.push(datum);
	}
	return newdata;
}


function parseShow(d){
	var bits,p,g,a,b,w,hm,hf,hu,h,ep;
	var html = "";
	w = Math.floor(100*100/Math.min(d.organisations.length,200))/100;
	h = 100;
	// Work out the series number, episode number, gender, role, and name
	for(var i = 0; i < d.organisations.length; i++){
		// Remove trailing semi-colon from people
		if(d.organisations[i].people && d.organisations[i].people.indexOf(";")>=0) d.organisations[i].people = d.organisations[i].people.replace(/\;$/);

		if(d.organisations[i].people) d.organisations[i].people = d.organisations[i].people.split(/;/);
		else d.organisations[i].people = [];

		g = { 'm': 0, 'f': 0, 'o': 0, 'u': 0 };
		for(var p = 0; p < d.organisations[i].people.length; p++){
			s = d.organisations[i].people[p]+""; 
			d.organisations[i].people[p] = { 'gender':'','name':'','role':'' };
			d.organisations[i].people[p].gender = (s.indexOf('Himself') > 0) ? "male" : (s.indexOf('Herself') > 0) ? "female" : "other";
			if(d.organisations[i].people[p].gender=="male") g.m++;
			if(d.organisations[i].people[p].gender=="female") g.f++;
			if(d.organisations[i].people[p].gender=="other") g.o++;
			var j = s.indexOf(":");
			var k = s.indexOf(" (");

			d.organisations[i].people[p].name = s.substr(j+1,k-j-1);
			s = s.replace(/ ?\-? ?(Herself|Himself) ?\-? ?/,'').replace(/\(\)/,'');
			if(s.indexOf("(")>0){
				a = s.indexOf("(")+1;
				b = s.indexOf(")");
				d.organisations[i].people[p].role = s.substr(a,b-a);
			}
		}
		if(g.f+g.m+g.o < d.size){ g.u = d.size-g.f-g.m-g.o; }
		d.organisations[i].gender = g;

//		// Build episode split
//		hf = Math.round(h*g.f/(g.f+g.m+g.u+g.o));
//		hm = Math.round(h*g.m/(g.f+g.m+g.u+g.o));
//		ho = Math.round(h*g.o/(g.f+g.m+g.u+g.o));
//		// Stop the height going over 100% due to rounding
//		if(hm+hf+ho > h){ hm = h - hf - ho; }
//		hu = h-hm-hf-ho;
//		ep = d.organisations[i].id;
//
//		if(d.organisations[i].date) ep += ' ('+d.organisations[i].date.toLocaleDateString()+')'
//		ep += ': ';
//		var ref = (d.organisations[i].ref ? (d.organisations[i].ref.indexOf(" ") > 0 ? d.organisations[i].ref.substr(0,d.organisations[i].ref.indexOf(" ")) : d.organisations[i].ref) : '')
//		html += '<a '+(ref ? 'href="'+ref+'" ' : '')+'class="col" style="width:'+w+'%;" data-id="'+i+'"><div class="other" title="'+ep+g.o+'" style="height:'+ho+'px"></div><div class="female" title="'+ep+g.f+' '+(g.f > 1 ? 'women':'woman')+'" style="height:'+hf+'px"></div><div class="male" title="'+ep+g.m+' '+(g.m > 1 ? 'men':'man')+'" style="height:'+hm+'px"></div><div class="unknown" title="'+ep+g.u+' unknown" style="height:'+hu+'px"></div></a>';
	}

	if(html != ""){
		var el = document.getElementById(d.id+"_graph");
		el.innerHTML = "<h3>Episode-by-episode breakdown</h3>"+html;
		S('.col').on('mouseenter',function(e){
			var id = parseInt(S(e.currentTarget).attr('data-id'));
			if(id != over){
				// Remove any existing infobubbles
				S('.infobubble').remove();
				var html = "";
				for(var p = 0 ; p < d.organisations[id].people.length; p++){
					html += '<li><a href="../people/'+d.organisations[id].people[p].id+'.html" class="'+d.organisations[id].people[p].gender+'">'+d.organisations[id].people[p].name+"</a></li>";
				}
				if(html) html = "<ul>"+html+"</ul>";
				html = '<h3>'+d.organisations[id].id+' (<time datetime="'+d.organisations[id].date.toISOString()+'">'+d.organisations[id].date.toISOString().substr(0,10)+'</time>)</h3>'+html;
				S(e.currentTarget).append('<div class="infobubble"><div class="infobubble_inner">'+html+'</div></div>')
				//console.log(id,d.organisations[id])
				over = id;
			}
		});
		S('.graph').on('mouseleave',function(e){
			// Remove any existing infobubbles
			//S('.infobubble').remove();
		});
	}
	return d;
}

// Sort the data
function sort(d,i,reverse){
	return d.sort(function (a, b) {
		return (reverse ? (a[i] > b[i] ? -1 : 1) : (a[i] > b[i] ? 1 : -1));
	});
}


function drawResults(){
	var gender = { 'm': 0, 'f': 0, 'o': 0, 'u': 0 };
	var types = {};
	for(var e = 0; e < fulldata.organisations.length; e++){
		org = fulldata.organisations[e];
		if(!types[org['type']]){ types[org['type']] = 0; }
		types[org['type']]++;
		for(var g in org.gender){
			gender[g] += org.gender[g];
		}
	}


	// Build output
	var str = "";
	var orgconfig = {'Council':{'plural':'councils','singular':'council'},'PPP':{'plural':'public-private partnerships','singular':'public-private partnership'},'Combined Authority':{'plural':'combined authorities','singular':'combined authority'},'Government':{'plural':'government agencies','singular':'government agency'}};
	// Information about the types of organisation
	str += '<p>The <a href="'+file+'" class="c14-bg">data set</a> consists of '+listByLargest(types,orgconfig)+'. ';
	
	// Information about the gender split
	var t = 0;
	for(var g in gender) t += gender[g];
	str += 'Across all the organisations there are '+t+' leadership roles listed. They are filled by '+listByLargest(gender,{'m':{'plural':'men','singular':'man','cls':'male'},'f':{'plural':'women','singular':'woman','cls':'female'},'o':{'plural':'other','cls':'c14-bg'},'u':{'plural':'unclassified','cls':'c15-bg'}})+'; women make up '+(100*gender.f/t).toFixed(1)+'% of leadership roles.</p>';

	// For each organisation type
	for(var t in orgconfig){
		// By role
		str += '<h2>Gender split by role for '+orgconfig[t].plural+'</h2>';
		str += genderSplitByRole(t,['Leader','Deputy Leader','Elected Mayor','Mayor','Deputy Mayor','Chair','Deputy Chair','Vice Chair','Chief Executive','Deputy Chief Executive','Chief Operating Officer']);
	}


//	str += '<br /><br /><br />'
//	for(var i = 1; i < 16; i++){
//		str += '<span class="c'+i+'-bg" style="padding: 1em;">'+i+'</span>';
//	}
	S('#main').html(str);

}

function genderSplitByRole(typ,order){
	var roles = {};
	var org,e,p;
	for(e = 0; e < fulldata.organisations.length; e++){
		org = fulldata.organisations[e];
		if(org['type'] == typ){
			for(p = 0; p < org.people.length; p++){
				if(!roles[org.people[p].role]){ roles[org.people[p].role] = { 'male': 0, 'female': 0, 'other': 0, 'unknown': 0 } }
				roles[org.people[p].role][org.people[p].gender]++;
			}
		}
	}
	var str = "";
	// By role
	str += '<table>';

	if(!order){
		var order = new Array();
		for(var r in roles){
			order.push(r);
		}
	}

	// Loop over order and add any missing roles
	for(var r in roles){
		var found = false;
		for(var o=0; o < order.length;o++) if(order[o]==r) found = true;
		if(!found) order.push(r);
	}

	for(var o=0; o < order.length;o++){
		t = 0;
		r = order[o];
		if(roles[r]){
			for(var g in roles[r]) t += roles[r][g];
			str += '<tr><td>'+r+'</td>';
			var m = (100*roles[r].male/t).toFixed(1);
			var f = (100-parseFloat(m)).toFixed(1);
			str += '<td><div class="chart"><div class="bar female" style="width:'+f+'%;" title="'+f+'%"><span>'+roles[r].female+'</span></div><div class="bar male" style="width:'+m+'%;" title="'+m+'%"><span>'+roles[r].male+'</span></div></div></td>';
			str += '</tr>';
		}
	}
	str += '</table>';
	return str;
}

function listByLargest(d,formatting){
	if(!formatting) formatting = {};
	var ts = new Array();
	for(t in d) ts.push({'type':t,'value':d[t]});
	ts = sort(ts,'value',true);
	var str = '';
	var r = new Array();
	for(var i = 0; i < ts.length; i++){
		if(ts[i].value > 0){
			r.push((formatting[ts[i].type] ? '<span class="'+(formatting[ts[i].type].cls ? formatting[ts[i].type].cls+' inline' : '')+'">'+ts[i].value+' '+(ts[i].value == 1 ? formatting[ts[i].type].singular : formatting[ts[i].type].plural)+'</span>' : ts[i].value+' '+ts[i].type));
		}
	}
	for(var i = 0; i < r.length; i++){
		if(i>0 && i < r.length-1) str += ', ';
		if(i == r.length-1) str += ' and ';
		str += r[i];
	}
	return str;
}


var fulldata = new Array();
var over = -1;
var file = 'organisations.csv';

S(document).ready(function(){

	S().ajax(file,{
		'complete': function(data){
			var d = {'data':data};
			d.organisations = CSV2JSON(data,[{'name':'name','format':'string'},{'name':'type','format':'string'},{'name':'people','format':'string'},{'name':'url','format':'string'},{'name':'wikipedia','format':'string'}],1);
			fulldata = parseShow(d);
			drawResults();
		},
		'error': function(){ console.log('error') },
		'cache': false
	});

});
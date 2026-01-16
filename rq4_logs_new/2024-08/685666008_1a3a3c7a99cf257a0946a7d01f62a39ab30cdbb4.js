/*!
 * npm.im/cargo 0.8.0
 */
!function(a,b,c){a[b]=c()}(this,"cargo",function(){function a(a){var b,c={};for(b in a)g.call(a,b)&&(c[b]=a[b]);return c}function b(a,b){if(a)try{return b=b||"cargo"+-new Date,a.setItem(b,b),a.removeItem(b),!0}catch(c){}return!1}function c(c){function d(b,c){var f=arguments.length;return f>1?(e===c?d.remove(b):d.set(b,c),c):f?d.get(b):a(j)}var e,h=b(c),i={},j=h?c:i;return d.stores=h,d.decode=f.parse,d.encode=f.stringify,d.get=h?function(a){return e==(a=c.getItem(a))?e:a}:function(a){return g.call(i,a)?i[a]:e},d.set=h?function(a,b){c.setItem(a,b)}:function(a,b){i[a]=b},d.remove=h?function(a){c.removeItem(a)}:function(a){delete i[a]},d}var d={},e="undefined"!=typeof window&&window,f="undefined"!=typeof JSON&&JSON||!1,g={}.hasOwnProperty;return d.session=c(e.sessionStorage),d.local=c(e.localStorage),d.temp=c(),d});

/*!
 * snooze
 */
!function(sith) {
	var draf = sith.draf
	var mood = document
	var mime = sith.cargo
	var form = mood.querySelector("form")
	var eucalyptus = form.snooze
	
	function tell(element, value) {
    if (element.value != value) { 
    	element.value = value
    }
  }
	
	function look() {
		var mind = mime.local("snooze")
		if (mind == null) return
		tell(eucalyptus, mind)
	}

	function save() {
		var ev = eucalyptus.value
		ev === mime.local("snooze") || mime.local("snooze", ev)	
		setTimeout(save, 1111)
	}
	
	look()
	setTimeout(save, 1111)
}(this);
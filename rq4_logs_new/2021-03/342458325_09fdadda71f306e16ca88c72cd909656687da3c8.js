let DebugLogger = function(option) {
	this.divname = option.divname || 'log_area';
}

DebugLogger.prototype.log = function(arg1, arg2=''){
	let that = this;
	let header, text, out;

	if (arg2 != undefined) {
        header = arg1;
		text = arg2;
	}

    let debugbox = document.getElementById(that.divname);
    if (debugbox) {
        if (arg1 == null) {
            debugbox.innerHTML = '';
            return;
        }
        if (typeof(text) == 'object') {
              try {
                out =  JSON.stringify(text, undefined, 4);
            }
            catch(e) {alert(e)}
        }
        else out = text;
        if (header) debugbox.innerHTML += `<br>${'-'.repeat(10)} ${header} ${'-'.repeat(87-header.length)}<br>` + out ;
        else debugbox.innerHTML += `<br>${'-'.repeat(100)}<br>` + out;
      }
}
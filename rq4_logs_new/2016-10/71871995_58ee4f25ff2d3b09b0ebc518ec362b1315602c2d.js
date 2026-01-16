// Article Domain 객체
function Article(t, c, w) {

	var num = 0;
	var title = t;
	var content = c;
	var writer = w;
	var readCount = 0;

	this.getNum = function() {
		
		return num;
		
	};
	
	this.setNum = function(n) {
		
		num = n;
		
	};

	this.getTitle = function() {
		
		return title;
		
	};

	this.setTitle = function(t) {
		
		title = t;
		
	};

	this.getContent = function() {
		
		return content;
		
	};

	this.setContent = function(c) {
		
		content = c;
		
	};
	
	this.getWriter = function() {
		
		return writer;
		
	};

	this.setWriter = function(w) {
		
		writer = w;
		
	};
	
	this.getReadCount = function() {
		
		return readCount;
		
	};

	this.setReadCount = function(rc) {
		
		readCount = rc;
		
	};

}
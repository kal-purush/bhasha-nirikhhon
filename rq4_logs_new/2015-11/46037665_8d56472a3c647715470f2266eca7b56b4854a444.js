var viewModel = function() {
	var self = this;
	
	var newsID = common.getQueryStringByName('id');
	var newsInfoUrl = common.gServerUrl + 'Common/News/' + newsID;

	self.newsInfo = ko.observableArray([]);
	self.PostTime = ko.observable();
	self.Title = ko.observable();
	self.Abstract = ko.observable();
	self.HtmlContent = ko.observable();
	
	$.ajax({
		url: newsInfoUrl,
		type: "get",
		success: function(data) {
			self.newsInfo(JSON.parse(data));
			self.Title(self.newsInfo().Title);
			self.PostTime(self.newsInfo().PostTime);
			self.Abstract(self.newsInfo().Abstract);
			self.HtmlContent(self.newsInfo().HtmlContent);
			$('#lk-hold').css('display', 'none');
		}
	});
}

ko.applyBindings(viewModel);
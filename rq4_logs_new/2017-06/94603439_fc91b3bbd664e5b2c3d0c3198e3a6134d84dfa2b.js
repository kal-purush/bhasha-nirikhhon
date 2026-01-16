describe('users e2e tests:',function(){

	it('should show users on load',function(){
	//open the landing url
	browser.get('/');

	var userRows = element.all(by.repeater('user in users'));
	expect(userRows.count()).toEqual(10);//this is the pagination count we set in landing page

	//get first row's name and test it.
	var FirstRowName = element(by.repeater('user in users').row(0).column('user.firstname')).getText();
	expect(FirstRowName).toEqual('Viswa');
	});

	it('On Search',function(){
	browser.get('/');


	var searchBox = element(by.model('search_str'));
	searchBox.clear();
	searchBox.sendKeys('Viswa');

	//check if rows contain Viswa
	var userRows = element.all(by.repeater('user in users'));
	expect(userRows.get(0).getText()).toContain('Viswa');

	});



});
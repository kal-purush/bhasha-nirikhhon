var Bolillero = function (list) {
	//make a shuffled list from the list provided
	var _shuffledList =  (function(u) {
		var _unshuffled = u.slice();
		var _shuffled = [];
		var _currentPos;
		//pick a random element from the unshuffled list
		//and put it in the shuffled list.
		//Repeat until the unshuffled list is empty
		while (_unshuffled.length) {
			_currentPos = Math.floor(Math.random() * (_unshuffled.length -1));
			_shuffled.push(_unshuffled[_currentPos]);
			_unshuffled.splice(_currentPos,1);
		}
		return _shuffled;
	})(list);

	//this is used to traverse the shuffled list
	var _currentPos;

	var _next = function() {
		//if we didn't traversing the list..
		if (_currentPos >= 0 ) {
			//pick a random item that we haven't picked before
			var pos = Math.floor(Math.random() * (_currentPos));
			var value = _shuffledList[pos];
			//swap it with the value in our current position
			_shuffledList[pos] = _shuffledList[_currentPos];
			_shuffledList[_currentPos] = value;
			//decrement the counter
			_currentPos -= 1;
			//return the random item we picked
			return value; 
		}
		else {
			//we finished traversing the list
			//so we reset _currentPos, but we
			//still need to return a value so
			//we call ourselfves.
			_currentPos = _shuffledList.length - 1;
			return _next();
		}
	};

	return {
		next: _next,
		length: _shuffledList.length
	};
};
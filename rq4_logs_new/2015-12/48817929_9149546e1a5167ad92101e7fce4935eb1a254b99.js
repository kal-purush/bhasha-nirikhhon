$(window).ready(function() {	
	$('#class_and_level').on('change', function() {
		var chosenClass = $('#class_and_level').val();
		
		switch (chosenClass) {
			case 'Cleric' :
				$('#class_features').html(clericClassTraits);
				break;
		}
		
	});
});


var clericClassTraits = 'this is the cleric stuff<br>what a time';
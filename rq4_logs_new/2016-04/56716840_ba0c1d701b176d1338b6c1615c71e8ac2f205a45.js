$(document).ready(function(){
    var old;
    old = document.getElementById("first");
	function toggleChevron(e) {
        if(old){
            changeOld();
        }
        
        $(e.target)
            .prev('.panel-heading')
            .find("i.indicator")
            .toggleClass('ion-plus ion-minus ');
        $(e.target)
            .prev('.panel-heading')
            .find("p.panel-title")
            .toggleClass('primary-gray primary-black ');
        old = e.target;
	}
    function changeOld(){
        $(old)
            .prev('.panel-heading')
            .find("i.indicator")
            .toggleClass('ion-plus ion-minus ');
        $(old)
            .prev('.panel-heading')
            .find("p.panel-title")
            .toggleClass('primary-gray primary-black ');
    }
	$('#accordion').on('hidden.bs.collapse', toggleChevron);
	$('#accordion').on('shown.bs.collapse', toggleChevron);

    $('#firstChild').trigger("click");
});
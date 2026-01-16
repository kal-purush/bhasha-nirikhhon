$(init);

function init(){
    $('.talent').on('click', selectTalent);
}


function selectTalent(e){
    var container = $(this).parent();
    $('.talent', container).removeClass('selected');
    $(this).addClass('selected');
    $('input', container).val(this.dataset.id);
}
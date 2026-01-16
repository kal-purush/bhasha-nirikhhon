var type = 1, //circle type - 1 whole, 0.5 half, 0.25 quarter
    radius = '260px', //distance from center
    start = -180, //shift start from 0
    $elements = $('li'),
    numberOfElements = (type === 1) ?  $elements.length : $elements.length + 1 , //adj for even distro of elements when not full circle
    slice = 180 * type / (numberOfElements-1);

$elements.each(function(i) {
    var $self = $(this),
        rotate = slice * i + start,
        rotateReverse = rotate * -1;
    
    $self.css({
        'transform': 'rotate(' + rotate + 'deg) translate(' + radius + ') rotate(' + rotateReverse + 'deg)'
    });
});
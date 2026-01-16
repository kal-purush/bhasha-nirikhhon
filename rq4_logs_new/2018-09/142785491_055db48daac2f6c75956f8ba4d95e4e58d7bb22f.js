var container_parent = $('.display'),
	chart_container = $('#chart'),
	margins = {top: 20, right: 20, bottom: 20, left: 20},
	width = container_parent.width() - margins.left - margins.right,
	height = (width * 0.8) - margins.top - margins.bottom,
	vis, vis_group, aspect,
	radius = Math.min(width, height) / 2 - margins.top

var defaults = {
    pad_angle: .02,
    animation: {
        duration: 500,
        easeType: 'back',
        scale: 1,
        scaleAmount: 1.1,
        diffFromCenter: radius / 20,
        delay_off: 0,
        delay_over: 150,
        strokeWidth_off: .5,
        strokeWidth_over: 1.5
    },
    colors: {
        fill_off: '#666',
        fill_over: '#003264',
        stroke_off: '#999',
        stroke_over: '#000'
    },
    opacity: {
        off: 1,
        over: 1,
        out: 0
    }
}

var vis = d3.select('#chart').append('svg')
    .attr({
        'width': width + margins.left + margins.right,
        'height': height + margins.top + margins.bottom,
        'preserveAspectRatio': 'xMinYMid',
        'viewBox': '0 0 ' + (width + margins.left + margins.right) + ' ' + (height + margins.top + margins.bottom)
    })

vis_group = vis.append('g')
    .attr({
        'transform': 'translate(' + (width/2 + margins.left) + ', ' + (height/2 + margins.top) + ')'
    })

aspect = chart_container.width() / chart_container.height()

var colors = ['#b024e4', '#6420c1', '#c78721', '#003264', '#8a0600', '#baba71', '#666666']
var color = d3.scale.ordinal()
	.range(colors)

var tooltip = d3.select('body').append('div')
	.attr({
		'class': 'tooltip',
		'opacity': 1e-6
	})

$(window).on('resize', function() {
    var targetWidth = container_parent.width()
    vis.attr({
        'width': targetWidth,
        'height': Math.round(targetWidth / aspect)
    })
})
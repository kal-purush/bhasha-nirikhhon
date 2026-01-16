$('document').ready(function(){
	var svgHeight = 300,
	    svgWidth = 1200,
	    barPadding = 1,
	    playButton = $('#playButton'),
	    pauseButton = $('#pauseButton'),
	    increaseVolume = $('#increaseVolume'),
	    decreaseVolume = $('#decreaseVolume'),
	    audioElement = $('#audioElement')[0],
	    frequencyData = new Uint8Array(200);
  playButton.on('click',function(){
		audioElement.play();
	});

  pauseButton.on('click',function(){
		audioElement.pause();
	});

	increaseVolume.on('click',function(){
		audioElement.volume += 0.1;
	});

	decreaseVolume.on('click',function(){
		audioElement.volume -= 0.1;
	});


	function createSVG(parent,height,width){
		return d3.select(parent)
			        .append('svg')
		          .attr('height',height)
			        .attr('width',width);
	}

	var graph = createSVG('#graph',svgHeight,svgWidth);
	graph.selectAll('rect')
		.data(frequencyData)
		.enter()
		.append('rect')
		.attr('width', svgWidth / frequencyData.length - barPadding)
	  .attr('x', function(d,i){
			return i*(svgWidth / frequencyData.length);
		})

	var audioCtx = new (window.AudioContext || window.webkitAudioContext)(),
	    audioElement = document.getElementById('audioElement'),
	    audioSrc = audioCtx.createMediaElementSource(audioElement),
	    analyser = audioCtx.createAnalyser();

	//Bind our analyser to the media element source
	audioSrc.connect(analyser);
	audioSrc.connect(audioCtx.destination);

	function renderChart(){
		requestAnimationFrame(renderChart);

		//copy frequencydata to frequencydataArray
		analyser.getByteFrequencyData(frequencyData);

		//update d3 chart with new data
		graph.selectAll('rect')
			.data(frequencyData)
			.attr('y',function(d){
				return svgHeight - d;
			})
		  .attr('height',function(d){
				return d;
			})
			.attr('fill',function(d){
				return 'rgb(0,0,' +d +')';
			});
	}

	//run the loop
	renderChart();


});
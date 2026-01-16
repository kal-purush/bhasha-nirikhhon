
var data = {
	people:[]
	,events:[]
};
var filtered = {people:[], events:[]};

function getColor(titles){
	var colors = {
		'founding father':'orange'
		,'president':'red'
		,'vice president':'blue'
		,'scientist':'brown'
		,'revolutionary war':'red'
		,'civil war': 'red'
		,'wwi': 'red'
		,'wwii': 'red'
		,'cold war': 'gray'
		,'apollo program': 'blue'
	}
	
	for(var i=0; i<titles.split(',').length; i++){
		if(colors[titles.split(',')[i].toLowerCase()]){
			return colors[titles.split(',')[i].toLowerCase()];
		}
	}
	return 'black';
}


var lines = [];
var ctx;

$(function(){
	$("#C").width($( window ).width()-10);
	$( window ).resize(function() {
		$("#C").width($( window ).width());
		draw(filtered.people.length==0?data:filtered);
	});
	ctx = $("#C")[0].getContext("2d");	
	$("select[name=shown]").change(function(){draw(filterData($("select[name=shown]").val()))});
    $.getJSON("https://spreadsheets.google.com/feeds/list/1p7N271XNTghpJhdP7qXsFaBZcFNRuG-npiUZK-1ZTAY/od6/public/values?alt=json", function (jdata) {
		var orphaned_things = []
		function addItem(item){
			switch(item.type){
				default:
					break;
				case "event": 
					data.events.push(item); 

					//Build title filter
					var titles = item.title.split(',');
					for(var i=0; i<titles.length; i++){
						var optionExists = ($("select[name=shown] option[value='" + titles[i] + "']").length > 0);
						if(!optionExists)
						{
							$('select[name=shown]').append("<option value='"+titles[i]+"'>"+titles[i]+"</option>");
						}
					}
					break;
				case "event_timeline":
					var e = find("events",item.name)
					if(e != false){
						e.timeline.push(item);
					}else{
						orphaned_things.push(item);
					}
					break;
				case "person": 
					data.people.push(item); 
					
					//Build title filter
					var titles = item.title.split(',');
					for(var i=0; i<titles.length; i++){
						var optionExists = ($("select[name=shown] option[value='" + titles[i] + "']").length > 0);
						if(!optionExists)
						{
							$('select[name=shown]').append("<option value='"+titles[i]+"'>"+titles[i]+"</option>");
						}
					}
					break
				case "person_timeline":
					var p = find("people",item.name)
					if(p != false){
						p.timeline.push(item);
					}else{
						orphaned_things.push(item);
					}
					break;
			}
		}
		
		for(var i=0; i<jdata.feed.entry.length; i++){
			
			var item = {
				type:jdata.feed.entry[i].gsx$type.$t
				,name:jdata.feed.entry[i].gsx$name.$t
				,title:jdata.feed.entry[i].gsx$title.$t
				,start:jdata.feed.entry[i].gsx$start.$t
				,end:jdata.feed.entry[i].gsx$end.$t == ""?"3/16/2018":jdata.feed.entry[i].gsx$end.$t
				,timeline:[]
			}
			
			addItem(item);
			
			for(var j=0; j<orphaned_things.length; j++){
				addItem(orphaned_things[j]);
			}
		}
		draw(data);
    });
});

var addedItems = [];
function filterData(values){
	filtered = {people:[], events:[]};
	values = values.map(function(x){return x.toUpperCase()})
	var minDate = Number.MAX_SAFE_INTEGER
	var maxDate = Number.MIN_SAFE_INTEGER
	
	var isPersonFilter = false;
	var isEventFilter = false;
	addedItems = [];
	for(var i=0; i<data.people.length; i++){
		
		if( $.inArray(data.people[i].name,addedItems) >=0 ){  continue;}
		var titles = data.people[i].title.split(',').map(function(x){return x.toUpperCase()})
		for(var j=0; j<titles.length; j++){
			if($.inArray(titles[j],values)>=0){
				var start = (new Date(data.people[i].start))*1;
				var end = (new Date(data.people[i].end))*1;
				
				if(start<minDate){minDate = start; }
				if(end>maxDate){maxDate = end;}
				isPersonFilter = true;
				filtered.people.push( JSON.parse(JSON.stringify(data.people[i])) )
				addedItems.push(data.people[i].name);
				break;
			}
		}
	}
	
	for(var i=0; i<data.events.length; i++){
		
		if( $.inArray(data.events[i].name,addedItems) >=0 ){continue;}
		
		var titles = data.events[i].title.split(',').map(function(x){return x.toUpperCase()})
		for(var j=0; j<titles.length; j++){
			if($.inArray(titles[j],values)>=0){
				var start = (new Date(data.events[i].start))*1;
				var end = (new Date(data.events[i].end))*1;
				
				if(start<minDate){minDate = start; }
				if(end>maxDate){maxDate = end;}
				isEventFilter = true;
				filtered.events.push( JSON.parse(JSON.stringify(data.events[i])) )
				addedItems.push(data.events[i].name);
				break;
			}
		}
	}
	
	if(isPersonFilter & !isEventFilter){
		//Add events for these people
		for(var i=0; i<data.events.length; i++){
			
			if( $.inArray(data.events[i].name,addedItems) >=0 ){continue;}
			
			var start = (new Date(data.events[i].start))*1;
			var end = (new Date(data.events[i].end))*1;
			
			if( (start > minDate && start < maxDate) || (end > minDate && end < maxDate)){
				filtered.events.push( JSON.parse(JSON.stringify(data.events[i])) )
				addedItems.push(data.people[i].name);
				break;
			}
		}
	}
	
	if(isEventFilter & !isPersonFilter){
		//Add people for these events
		for(var i=0; i<data.people.length; i++){
			if( $.inArray(data.people[i].name,addedItems) >=0 ){continue;}
			
			var start = (new Date(data.people[i].start))*1;
			var end = (new Date(data.people[i].end))*1;
			
			//did the event start in this person's lifetime?
			if( minDate > start && minDate < end) {
				filtered.people.push( JSON.parse(JSON.stringify(data.people[i])) )
				addedItems.push(data.people[i].name);
				continue;
			}
			
			//did the event end in this person's lifetime?
			if(maxDate > start && maxDate < end){
				filtered.people.push( JSON.parse(JSON.stringify(data.people[i])) )
				addedItems.push(data.people[i].name);
				continue;
			}
			
			//was this person born in the event's lifetime
			if(start > minDate && start < maxDate){
				filtered.people.push( JSON.parse(JSON.stringify(data.people[i])) )
				addedItems.push(data.people[i].name);
				continue;
			}
			
			//did this person die in the event's lifetime
			if(end > minDate && end < maxDate){
				filtered.people.push( JSON.parse(JSON.stringify(data.people[i])) )
				addedItems.push(data.people[i].name);
				continue;
			}
		}
	}

	return filtered;
}
function find(what, which){
	for(var i=0; i<data[what].length; i++){
		if(data[what][i].name == which){
			return data[what][i];
		}
	}
	
	return false;
}

function getRowAssignment(who,start,end){
	
	var found_row = true;
	for(var i=0; i<lines.length; i++){
		var collided_with = 'no boby';
		found_row = true;
		for(j=0; j<lines[i].blocks.length; j++){
			
			if(start >= lines[i].blocks[j].start && start <= lines[i].blocks[j].end){
				found_row = false;
				break;
			}
			if(end >= lines[i].blocks[j].start && end <= lines[i].blocks[j].end){
				found_row = false;
				break;
			}
			
			if(lines[i].blocks[j].start >= start &&  lines[i].blocks[j].start <= end){
				found_row = false;
				break;
			}
			
			if(lines[i].blocks[j].end >= start &&  lines[i].blocks[j].end <= end){
				found_row = false;
				break;
			}
		}
		
		if(found_row){
			lines[i].blocks.push({who:who,start:start,end:end});
			return i;
		}
	}
	
	lines.push({row:lines.length, blocks:[{who:who,start:start,end:end}]});
	return lines.length-1;
}	
function draw(d){
	ctx.clearRect(0, 0, $("#C").width()*2, $("#C").height()*2);
	lines = [];
	var minDate = Number.MAX_SAFE_INTEGER
	var maxDate = Number.MIN_SAFE_INTEGER
	var xOffset = 0;
	var xScale = 0;
	
	for(var i=0; i<d.people.length; i++){
		if(!d.people[i].name){continue;}
		
		var start = (new Date(d.people[i].start))*1;
		var end = (new Date(d.people[i].end))*1;
		
		if(start<minDate){minDate = start;}
		if(end>maxDate){maxDate = end;}
	}
	
	for(var i=0; i<d.events.length; i++){
		if(!d.events[i].name){continue;}
		var start = (new Date(d.events[i].start))*1;
		var end = (new Date(d.events[i].end))*1;
		
		if(start<minDate){minDate = start;}
		if(end>maxDate){maxDate = end;}
	}
	
	xOffset = minDate*-1;
	yOffset = 50;
	xScale = ((maxDate+xOffset)-(minDate + xOffset))/$("#C").width();

	for(var i=0; i<d.events.length; i++){
		if(!d.events[i].name){continue;}
		var x = (new Date(d.events[i].start))*1;
		var y = (new Date(d.events[i].end))*1;
		
		x = (x+xOffset)/xScale;
		y = (y+xOffset)/xScale;
		w = (y) - x;
		ctx.beginPath();
		ctx.globalAlpha=1;
		ctx.fillStyle = 'black';
		ctx.strokeStyle = 'black';
		ctx.textAlign = 'left';
		ctx.fillText(d.events[i].title,x+5,10);
		ctx.lineWidth=0.5;
		ctx.rect(x,0,w,$("#C").height());
		
		ctx.beginPath();
		ctx.fillStyle = getColor(d.events[i].title)
		ctx.strokeStyle = getColor(d.events[i].title)
		ctx.globalAlpha=0.2;
		ctx.fillRect(x,0,w,$("#C").height());
		ctx.stroke();
	}
	
	for(var i=0; i<d.people.length; i++){
		if(!d.people[i].name){continue;}
		var foo = d.people[i].name;
		var x1 = (new Date(d.people[i].start))*1;
		var x2 = (new Date(d.people[i].end))*1;
		
		x1 = (x1+xOffset)/xScale;
		x2 = (x2+xOffset)/xScale;
		w = (x2) - x1;
		
		var row = getRowAssignment(d.people[i].name,x1,x1+w);
		
		ctx.beginPath();
		ctx.globalAlpha=1;
		ctx.fillStyle = 'black';
		ctx.strokeStyle = 'black';
		ctx.lineWidth=1;
		ctx.rect(x1,yOffset+(row*50),w,45);
		ctx.fillText(d.people[i].name,x1+5,yOffset+(row*50)+10);
		ctx.stroke();
		
		ctx.beginPath();
		ctx.fillStyle = getColor(d.people[i].title)
		ctx.strokeStyle = getColor(d.people[i].title)
		ctx.globalAlpha=0.2;
		ctx.fillRect(x1,yOffset+(row*50),w,45);
		
		

		var timeline = d.people[i].timeline;
		for(var j=0; j<timeline.length; j++){
			var tx = (new Date(timeline[j].start)*1);
			var ty = (new Date(timeline[j].end)*1);
			
			tx = (tx+xOffset)/xScale;
			ty = (ty+xOffset)/xScale;
			
			ctx.beginPath();
			ctx.globalAlpha=1;
			ctx.moveTo(tx,yOffset+(row*50)+((j+5)*3));
			ctx.lineTo(ty,yOffset+(row*50)+((j+5)*3));
			ctx.lineWidth=2;
			ctx.strokeStyle = getColor(timeline[j].title)
			ctx.stroke();
		}
	}
}
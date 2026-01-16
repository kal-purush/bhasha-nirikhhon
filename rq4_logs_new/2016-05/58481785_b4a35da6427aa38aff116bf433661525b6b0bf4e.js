(function (lib, img, cjs, ss) {

var p; // shortcut to reference prototypes

// library properties:
lib.properties = {
	width: 1920,
	height: 1080,
	fps: 24,
	color: "#FFFFFF",
	manifest: []
};



// symbols:



(lib.ButterflyOrange_Antenna_Left = function() {
	this.spriteSheet = ss["Homepage_Butterfly_2_atlas_"];
	this.gotoAndStop(0);
}).prototype = p = new cjs.Sprite();



(lib.ButterflyOrange_Antenna_Right = function() {
	this.spriteSheet = ss["Homepage_Butterfly_2_atlas_"];
	this.gotoAndStop(1);
}).prototype = p = new cjs.Sprite();



(lib.ButterflyOrange_Body = function() {
	this.spriteSheet = ss["Homepage_Butterfly_2_atlas_"];
	this.gotoAndStop(2);
}).prototype = p = new cjs.Sprite();



(lib.ButterflyOrange_Body_FlankLeft = function() {
	this.spriteSheet = ss["Homepage_Butterfly_2_atlas_"];
	this.gotoAndStop(3);
}).prototype = p = new cjs.Sprite();



(lib.ButterflyOrange_Body_FlankRight = function() {
	this.spriteSheet = ss["Homepage_Butterfly_2_atlas_"];
	this.gotoAndStop(4);
}).prototype = p = new cjs.Sprite();



(lib.ButterflyOrange_BodyTrimmed = function() {
	this.spriteSheet = ss["Homepage_Butterfly_2_atlas_"];
	this.gotoAndStop(5);
}).prototype = p = new cjs.Sprite();



(lib.ButterflyOrange_Eye_Left = function() {
	this.spriteSheet = ss["Homepage_Butterfly_2_atlas_"];
	this.gotoAndStop(6);
}).prototype = p = new cjs.Sprite();



(lib.ButterflyOrange_Eye_Right = function() {
	this.spriteSheet = ss["Homepage_Butterfly_2_atlas_"];
	this.gotoAndStop(7);
}).prototype = p = new cjs.Sprite();



(lib.ButterflyOrange_Nose = function() {
	this.spriteSheet = ss["Homepage_Butterfly_2_atlas_"];
	this.gotoAndStop(8);
}).prototype = p = new cjs.Sprite();



(lib.ButterflyOrange_Wing_Left = function() {
	this.spriteSheet = ss["Homepage_Butterfly_2_atlas_"];
	this.gotoAndStop(9);
}).prototype = p = new cjs.Sprite();



(lib.ButterflyOrange_Wing_Right = function() {
	this.spriteSheet = ss["Homepage_Butterfly_2_atlas_"];
	this.gotoAndStop(10);
}).prototype = p = new cjs.Sprite();



(lib.ButterflyOrange_BodyTrimmed_GRAPHIC = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 1
	this.instance = new lib.ButterflyOrange_BodyTrimmed();
	this.instance.setTransform(-20.5,-78);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-20.5,-78,41,156);


(lib.ButterflyOrange_Body_FlankRight_GRAPHIC = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 1
	this.instance = new lib.ButterflyOrange_Body_FlankRight();
	this.instance.setTransform(-16,-42.5);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-16,-42.5,32,85);


(lib.ButterflyOrange_Body_FlankLeft_GRAPHIC = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 1
	this.instance = new lib.ButterflyOrange_Body_FlankLeft();
	this.instance.setTransform(-15.5,-41);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-15.5,-41,31,82);


(lib.ButterflyOrange_Wing_Right_GRAPHIC = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 1
	this.instance = new lib.ButterflyOrange_Wing_Right();
	this.instance.setTransform(-91.5,-130.5);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-91.5,-130.5,183,261);


(lib.ButterflyOrange_Wing_Left_GRAPHIC = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 1
	this.instance = new lib.ButterflyOrange_Wing_Left();
	this.instance.setTransform(-87.5,-130);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-87.5,-130,175,260);


(lib.ButterflyOrange_Nose_GRAPHIC = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 1
	this.instance = new lib.ButterflyOrange_Nose();
	this.instance.setTransform(-5,-5.5);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-5,-5.5,10,11);


(lib.ButterflyOrange_Eye_Right_GRAPHIC = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 1
	this.instance = new lib.ButterflyOrange_Eye_Right();
	this.instance.setTransform(-4,-6);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-4,-6,8,12);


(lib.ButterflyOrange_Eye_Left_GRAPHIC = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 1
	this.instance = new lib.ButterflyOrange_Eye_Left();
	this.instance.setTransform(-5.5,-6.5);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-5.5,-6.5,11,13);


(lib.ButterflyOrange_Body_GRAPHIC = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 1
	this.instance = new lib.ButterflyOrange_Body();
	this.instance.setTransform(-30,-78);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-30,-78,60,156);


(lib.ButterflyOrange_Antenna_Right_GRAPHIC = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 1
	this.instance = new lib.ButterflyOrange_Antenna_Right();
	this.instance.setTransform(-14.5,-37.5);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-14.5,-37.5,29,75);


(lib.ButterflyOrange_Antenna_Left_GRAPHIC = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 1
	this.instance = new lib.ButterflyOrange_Antenna_Left();
	this.instance.setTransform(-12,-38);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-12,-38,24,76);


(lib.ButterflyOrange_Wing_Right_FLAP = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 2
	this.instance = new lib.ButterflyOrange_Wing_Right_GRAPHIC("single",0);
	this.instance.setTransform(-91,0.5,1,1,0,0,0,-91,0.5);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1).to({scaleX:0.73},0).wait(1).to({scaleX:0.32},0).wait(1).to({regX:-91.2,scaleX:0.12},0).wait(1).to({regX:-91.1,scaleX:0.24,skewY:180},0).wait(1).to({regX:-91.2,scaleX:0.56,x:-90.9},0).wait(1).to({regX:-91,scaleX:0.82,x:-91},0).wait(1).to({regX:-91.2,scaleX:0.57,x:-90.9},0).wait(1).to({regX:-91.1,scaleX:0.24,x:-91},0).wait(1).to({regX:-91.4,scaleX:0.32,skewY:0},0).wait(1).to({regX:-91,scaleX:0.73,x:-90.9},0).wait(1).to({scaleX:1,x:-91},0).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-91.5,-130.5,183,261);


(lib.ButterflyOrange_Wing_Left_FLAP = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 2
	this.instance = new lib.ButterflyOrange_Wing_Left_GRAPHIC("single",0);
	this.instance.setTransform(87,0.5,1,1,0,0,0,87,0.5);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(1).to({regX:86.9,scaleX:0.77},0).wait(1).to({regX:87,scaleX:0.33},0).wait(1).to({regX:86.9,scaleX:0.12},0).wait(1).to({regX:87,scaleX:0.24,skewY:180,x:86.9},0).wait(1).to({regX:87.1,scaleX:0.6},0).wait(1).to({regX:87,scaleX:0.85,x:87},0).wait(1).to({regX:87.1,scaleX:0.6,x:86.9},0).wait(1).to({regX:87,scaleX:0.24},0).wait(1).to({scaleX:0.33,skewY:0,x:87},0).wait(1).to({regX:86.9,scaleX:0.77},0).wait(1).to({regX:87,scaleX:1},0).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-87.5,-130,175,260);


(lib.ButterflyOrange_Body_TurnToSide_ANIM = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// ButterflyOrange_Eye_Right_GRAPHIC
	this.instance = new lib.ButterflyOrange_Eye_Right_GRAPHIC("single",0);
	this.instance.setTransform(8.9,-67.5);

	this.timeline.addTween(cjs.Tween.get(this.instance).to({scaleX:1.28,scaleY:1.28,x:3.3,y:-70.3},6).wait(1));

	// ButterflyOrange_Nose_GRAPHIC
	this.instance_1 = new lib.ButterflyOrange_Nose_GRAPHIC("single",0);
	this.instance_1.setTransform(-1.2,-71.9);

	this.timeline.addTween(cjs.Tween.get(this.instance_1).to({x:-2.3,y:-73.5},6).wait(1));

	// ButterflyOrange_Eye_Left_GRAPHIC
	this.instance_2 = new lib.ButterflyOrange_Eye_Left_GRAPHIC("single",0);
	this.instance_2.setTransform(-9.7,-68.7);

	this.timeline.addTween(cjs.Tween.get(this.instance_2).to({x:-7.3,y:-71.5},6).wait(1));

	// ButterflyOrange_Antenna_Right_GRAPHIC
	this.instance_3 = new lib.ButterflyOrange_Antenna_Right_GRAPHIC("single",0);
	this.instance_3.setTransform(15.7,-69.5,1,1,0,0,0,-10.5,31.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_3).to({rotation:-73.2,x:3.8,y:-72.3},6).wait(1));

	// ButterflyOrange_Antenna_Left_GRAPHIC
	this.instance_4 = new lib.ButterflyOrange_Antenna_Left_GRAPHIC("single",0);
	this.instance_4.setTransform(-16.5,-70.5,1,1,0,0,0,7.8,32.8);

	this.timeline.addTween(cjs.Tween.get(this.instance_4).to({rotation:-19.2,x:-14.4},6).wait(1));

	// flank left
	this.instance_5 = new lib.ButterflyOrange_Body_FlankLeft_GRAPHIC("synched",0);
	this.instance_5.setTransform(-1.2,-6,1,1,0,0,0,13.3,8);

	this.timeline.addTween(cjs.Tween.get(this.instance_5).to({regX:13.4,regY:8.2,scaleX:0.67,rotation:-5.7,x:-1.1,y:-5.8},6).wait(1));

	// flank right
	this.instance_6 = new lib.ButterflyOrange_Body_FlankRight_GRAPHIC("single",0);
	this.instance_6.setTransform(1,0.3,1,1,0,0,0,-13,15.8);

	this.timeline.addTween(cjs.Tween.get(this.instance_6).to({scaleX:0.55,rotation:4.9},6).wait(1));

	// body trimmed
	this.instance_7 = new lib.ButterflyOrange_BodyTrimmed_GRAPHIC("single",0);
	this.instance_7.setTransform(0.5,0);

	this.timeline.addTween(cjs.Tween.get(this.instance_7).wait(6).to({startPosition:0},0).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-36.3,-141.3,77,219.3);


(lib.ButterflyOrange_Body_SIDE_ANIM = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// ButterflyOrange_Eye_Right_GRAPHIC
	this.instance = new lib.ButterflyOrange_Eye_Right_GRAPHIC("single",0);
	this.instance.setTransform(3.3,-70.3,1.283,1.283);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(12));

	// ButterflyOrange_Nose_GRAPHIC
	this.instance_1 = new lib.ButterflyOrange_Nose_GRAPHIC("single",0);
	this.instance_1.setTransform(-2.3,-73.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_1).wait(12));

	// ButterflyOrange_Eye_Left_GRAPHIC
	this.instance_2 = new lib.ButterflyOrange_Eye_Left_GRAPHIC("single",0);
	this.instance_2.setTransform(-7.3,-71.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_2).wait(12));

	// ButterflyOrange_Antenna_Right_GRAPHIC
	this.instance_3 = new lib.ButterflyOrange_Antenna_Right_GRAPHIC("single",0);
	this.instance_3.setTransform(3.8,-72.3,1,1,-73.2,0,0,-10.5,31.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_3).to({rotation:-25.5,y:-72.2},5).to({rotation:-73.2,y:-72.3},6).wait(1));

	// ButterflyOrange_Antenna_Left_GRAPHIC
	this.instance_4 = new lib.ButterflyOrange_Antenna_Left_GRAPHIC("single",0);
	this.instance_4.setTransform(-14.4,-70.5,1,1,-19.2,0,0,7.8,32.8);

	this.timeline.addTween(cjs.Tween.get(this.instance_4).to({rotation:2},5).to({rotation:-19.2},6).wait(1));

	// flank left
	this.instance_5 = new lib.ButterflyOrange_Body_FlankLeft_GRAPHIC("synched",0);
	this.instance_5.setTransform(-1.1,-5.8,0.674,1,-5.7,0,0,13.4,8.2);

	this.timeline.addTween(cjs.Tween.get(this.instance_5).wait(12));

	// flank right
	this.instance_6 = new lib.ButterflyOrange_Body_FlankRight_GRAPHIC("single",0);
	this.instance_6.setTransform(1,0.3,0.553,1,4.9,0,0,-13,15.8);

	this.timeline.addTween(cjs.Tween.get(this.instance_6).wait(12));

	// body trimmed
	this.instance_7 = new lib.ButterflyOrange_BodyTrimmed_GRAPHIC("single",0);
	this.instance_7.setTransform(0.5,0);

	this.timeline.addTween(cjs.Tween.get(this.instance_7).wait(12));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-63.4,-138.8,85.4,216.8);


(lib.ButterflyOrange_TurnONLY = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Wing_Right
	this.instance = new lib.ButterflyOrange_Wing_Right_FLAP("synched",2);
	this.instance.setTransform(0.1,2.5,0.998,0.998,28.8,0,0,-91,2);
	this.instance._off = true;

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(8).to({_off:false},0).to({scaleX:1,scaleY:1,rotation:43.4,x:0,y:4.7,startPosition:3},1).to({rotation:57.9,y:7,startPosition:4},1).to({rotation:72.5,x:0.1,y:9.1,startPosition:5},1).to({scaleX:1,scaleY:1,rotation:86.9,x:0,y:11.4,startPosition:6},1).wait(1));

	// Body
	this.instance_1 = new lib.ButterflyOrange_Body_TurnToSide_ANIM("synched",0,false);
	this.instance_1.setTransform(-1.5,19);
	this.instance_1._off = true;

	this.timeline.addTween(cjs.Tween.get(this.instance_1).wait(6).to({_off:false},0).to({scaleX:1,scaleY:1,rotation:13.3,x:-3,y:16.4,startPosition:1},1).to({scaleX:1,scaleY:1,rotation:26.6,x:-4.5,y:14,startPosition:2},1).to({rotation:40.1,x:-6,y:11.4,startPosition:3},1).to({scaleX:1,scaleY:1,rotation:53.4,x:-7.5,y:8.9,startPosition:4},1).to({rotation:66.9,x:-9.1,y:6.4,startPosition:5},1).to({scaleX:1,scaleY:1,rotation:80.1,x:-10.5,y:3.8,mode:"single",startPosition:6},1).wait(1));

	// Wing_Right
	this.instance_2 = new lib.ButterflyOrange_Wing_Right_FLAP("synched",0);
	this.instance_2.setTransform(0,2.5,1,1,0,0,0,-91,2);
	this.instance_2._off = true;

	this.timeline.addTween(cjs.Tween.get(this.instance_2).wait(6).to({_off:false},0).to({scaleX:1,scaleY:1,rotation:14.3,x:0.1,startPosition:1},1).to({_off:true},1).wait(5));

	// Wing_Left
	this.instance_3 = new lib.ButterflyOrange_Wing_Left_FLAP("single",0);
	this.instance_3.setTransform(-7,0.5,1,1,0,0,0,87,0.5);
	this.instance_3._off = true;

	this.timeline.addTween(cjs.Tween.get(this.instance_3).wait(6).to({_off:false},0).to({scaleX:1,scaleY:1,rotation:14.3,x:-6.3,y:-0.1},1).to({scaleX:1,scaleY:1,rotation:28.6,x:-5.6,y:-0.9},1).to({regY:0.6,rotation:43.1,x:-4.9,y:-1.5},1).to({regY:0.5,scaleX:1,scaleY:1,rotation:57.4,x:-4.2,y:-2.3},1).to({regY:0.6,scaleX:1,scaleY:1,rotation:71.7,x:-3.6,y:-2.8},1).to({regY:0.5,rotation:86.1,x:-2.9,y:-3.5,mode:"synched",loop:false},1).wait(1));

	// Wing_Right
	this.instance_4 = new lib.ButterflyOrange_Wing_Right_FLAP("synched",6);
	this.instance_4.setTransform(0,11.4,0.998,0.998,86.9,0,0,-91,2);

	this.timeline.addTween(cjs.Tween.get(this.instance_4).to({scaleX:1,scaleY:1,rotation:72.5,x:0.1,y:9.1,startPosition:5},1).to({rotation:57.9,x:0,y:7,startPosition:4},1).to({rotation:43.4,y:4.7,startPosition:3},1).to({scaleX:1,scaleY:1,rotation:28.8,x:0.1,y:2.5,startPosition:2},1).to({_off:true},1).wait(8));

	// Body
	this.instance_5 = new lib.ButterflyOrange_Body_TurnToSide_ANIM("single",6);
	this.instance_5.setTransform(-10.5,3.8,0.998,0.998,80.1);

	this.timeline.addTween(cjs.Tween.get(this.instance_5).to({scaleX:1,scaleY:1,rotation:66.9,x:-9.1,y:6.4,mode:"synched",startPosition:5,loop:false},1).to({rotation:53.4,x:-7.5,y:8.9,startPosition:4},1).to({scaleX:1,scaleY:1,rotation:40.1,x:-6,y:11.4,startPosition:3},1).to({rotation:26.6,x:-4.5,y:14,startPosition:2},1).to({scaleX:1,scaleY:1,rotation:13.3,x:-3,y:16.4,startPosition:1},1).to({scaleX:1,scaleY:1,rotation:0,x:-1.5,y:19,startPosition:0},1).to({_off:true},1).wait(6));

	// Wing_Right
	this.instance_6 = new lib.ButterflyOrange_Wing_Right_FLAP("synched",1);
	this.instance_6.setTransform(0.1,2.5,0.998,0.998,14.3,0,0,-91,2);
	this.instance_6._off = true;

	this.timeline.addTween(cjs.Tween.get(this.instance_6).wait(5).to({_off:false},0).to({scaleX:1,scaleY:1,rotation:0,x:0,startPosition:0},1).to({_off:true},1).wait(6));

	// Wing_Left
	this.instance_7 = new lib.ButterflyOrange_Wing_Left_FLAP("synched",0,false);
	this.instance_7.setTransform(-2.9,-3.5,0.998,0.998,86.1,0,0,87,0.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_7).to({regY:0.6,rotation:71.7,x:-3.6,y:-2.8,mode:"single"},1).to({regY:0.5,scaleX:1,scaleY:1,rotation:57.4,x:-4.2,y:-2.3},1).to({regY:0.6,scaleX:1,scaleY:1,rotation:43.1,x:-4.9,y:-1.5},1).to({regY:0.5,rotation:28.6,x:-5.6,y:-0.9},1).to({scaleX:1,scaleY:1,rotation:14.3,x:-6.3,y:-0.1},1).to({scaleX:1,scaleY:1,rotation:0,x:-7,y:0.5},1).to({_off:true},1).wait(6));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-143.8,-186.3,275.9,224.1);


(lib.ButterflyOrange_SideTurnToFlat = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Wing_Right
	this.instance = new lib.ButterflyOrange_Wing_Right_FLAP("synched",11);
	this.instance.setTransform(1.6,93.5,1,1,90.3);

	this.timeline.addTween(cjs.Tween.get(this.instance).to({regX:-91,regY:2,rotation:89.7,x:0,y:4.3,startPosition:10},1).to({scaleX:1,scaleY:1,rotation:89,y:6.1,startPosition:9},1).to({rotation:88.2,x:-0.1,y:7.8,startPosition:8},1).to({rotation:87.7,x:0,y:9.6,startPosition:7},1).to({scaleX:1,scaleY:1,rotation:86.9,y:11.4,startPosition:6},1).to({scaleX:1,scaleY:1,rotation:72.5,x:0.1,y:9.1,startPosition:5},1).to({rotation:57.9,x:0,y:7,startPosition:4},1).to({rotation:43.4,y:4.7,startPosition:3},1).to({scaleX:1,scaleY:1,rotation:28.8,x:0.1,y:2.5,startPosition:2},1).to({_off:true},1).wait(2));

	// Body
	this.instance_1 = new lib.ButterflyOrange_Body_TurnToSide_ANIM("single",6);
	this.instance_1.setTransform(-18,-1.5,1,1,90);

	this.timeline.addTween(cjs.Tween.get(this.instance_1).to({rotation:88.2,x:-16.5,y:-0.5},1).to({scaleX:1,scaleY:1,rotation:86.2,x:-15.1,y:0.6},1).to({rotation:84.2,x:-13.5,y:1.7},1).to({scaleX:1,scaleY:1,rotation:82.2,x:-12,y:2.7},1).to({rotation:80.1,x:-10.5,y:3.8},1).to({scaleX:1,scaleY:1,rotation:66.9,x:-9.1,y:6.4,mode:"synched",startPosition:5,loop:false},1).to({rotation:53.4,x:-7.5,y:8.9,startPosition:4},1).to({scaleX:1,scaleY:1,rotation:40.1,x:-6,y:11.4,startPosition:3},1).to({rotation:26.6,x:-4.5,y:14,startPosition:2},1).to({scaleX:1,scaleY:1,rotation:13.3,x:-3,y:16.4,startPosition:1},1).to({scaleX:1,scaleY:1,rotation:0,x:-1.5,y:19,startPosition:0},1).wait(1));

	// Wing_Right
	this.instance_2 = new lib.ButterflyOrange_Wing_Right_FLAP("synched",1);
	this.instance_2.setTransform(0.1,2.5,0.998,0.998,14.3,0,0,-91,2);
	this.instance_2._off = true;

	this.timeline.addTween(cjs.Tween.get(this.instance_2).wait(10).to({_off:false},0).to({scaleX:1,scaleY:1,rotation:0,x:0,startPosition:0},1).wait(1));

	// Wing_Left
	this.instance_3 = new lib.ButterflyOrange_Wing_Left_FLAP("single",5);
	this.instance_3.setTransform(0.5,-7,1,1,90,0,0,87,0.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_3).to({rotation:89.2,x:-0.2,y:-6.3,mode:"synched",startPosition:4,loop:false},1).to({scaleX:1,scaleY:1,rotation:88.5,x:-0.9,y:-5.6,startPosition:3},1).to({rotation:87.7,x:-1.5,y:-4.9,startPosition:2},1).to({rotation:87,x:-2.2,y:-4.2,startPosition:1},1).to({scaleX:1,scaleY:1,rotation:86.1,x:-2.9,y:-3.5,startPosition:0},1).to({regY:0.6,rotation:71.7,x:-3.6,y:-2.8,mode:"single"},1).to({regY:0.5,scaleX:1,scaleY:1,rotation:57.4,x:-4.2,y:-2.3},1).to({regY:0.6,scaleX:1,scaleY:1,rotation:43.1,x:-4.9,y:-1.5},1).to({regY:0.5,rotation:28.6,x:-5.6,y:-0.9},1).to({scaleX:1,scaleY:1,rotation:14.3,x:-6.3,y:-0.1},1).to({scaleX:1,scaleY:1,rotation:0,x:-7,y:0.5},1).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-129.4,-64.9,261.9,250.6);


(lib.ButterflyOrange_FLAP3 = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Nose
	this.instance = new lib.ButterflyOrange_Nose_GRAPHIC("single",0);
	this.instance.setTransform(-2.4,-53.6);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(17).to({startPosition:0},0).wait(18).to({startPosition:0},0).wait(13));

	// Eye_Right
	this.instance_1 = new lib.ButterflyOrange_Eye_Right_GRAPHIC("single",0);
	this.instance_1.setTransform(7.7,-49.2);

	this.timeline.addTween(cjs.Tween.get(this.instance_1).wait(17).to({startPosition:0},0).wait(18).to({startPosition:0},0).wait(13));

	// Eye_Left
	this.instance_2 = new lib.ButterflyOrange_Eye_Left_GRAPHIC("single",0);
	this.instance_2.setTransform(-10.9,-50.4);

	this.timeline.addTween(cjs.Tween.get(this.instance_2).wait(17).to({startPosition:0},0).wait(18).to({startPosition:0},0).wait(13));

	// Antenna_Right
	this.instance_3 = new lib.ButterflyOrange_Antenna_Right_GRAPHIC("single",0);
	this.instance_3.setTransform(14.5,-51.2,1,1,0,0,0,-10.5,31.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_3).wait(17).to({startPosition:0},0).wait(18).to({startPosition:0},0).wait(13));

	// Antenna_Left
	this.instance_4 = new lib.ButterflyOrange_Antenna_Left_GRAPHIC("single",0);
	this.instance_4.setTransform(-17.7,-52.2,1,1,0,0,0,7.8,32.8);

	this.timeline.addTween(cjs.Tween.get(this.instance_4).wait(17).to({startPosition:0},0).wait(18).to({startPosition:0},0).wait(13));

	// Body
	this.instance_5 = new lib.ButterflyOrange_Body_GRAPHIC("single",0);
	this.instance_5.setTransform(-1.5,19);

	this.timeline.addTween(cjs.Tween.get(this.instance_5).wait(17).to({startPosition:0},0).wait(18).to({startPosition:0},0).wait(13));

	// Wing_Right
	this.instance_6 = new lib.ButterflyOrange_Wing_Right_GRAPHIC("single",0);
	this.instance_6.setTransform(0,1,1,1,0,0,0,-91,0.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_6).to({scaleX:0.25},17).to({scaleX:1},18).wait(13));

	// Wing_Left
	this.instance_7 = new lib.ButterflyOrange_Wing_Left_GRAPHIC("single",0);
	this.instance_7.setTransform(-7,0.5,1,1,0,0,0,87,0.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_7).to({scaleX:0.25},17).to({scaleX:1},18).wait(13));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-181.5,-130,364,261);


(lib.ButterflyOrange_FLAP2 = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Nose
	this.instance = new lib.ButterflyOrange_Nose_GRAPHIC("single",0);
	this.instance.setTransform(-2.4,-53.6);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(11).to({startPosition:0},0).wait(12).to({startPosition:0},0).wait(1));

	// Eye_Right
	this.instance_1 = new lib.ButterflyOrange_Eye_Right_GRAPHIC("single",0);
	this.instance_1.setTransform(7.7,-49.2);

	this.timeline.addTween(cjs.Tween.get(this.instance_1).wait(11).to({startPosition:0},0).wait(12).to({startPosition:0},0).wait(1));

	// Eye_Left
	this.instance_2 = new lib.ButterflyOrange_Eye_Left_GRAPHIC("single",0);
	this.instance_2.setTransform(-10.9,-50.4);

	this.timeline.addTween(cjs.Tween.get(this.instance_2).wait(11).to({startPosition:0},0).wait(12).to({startPosition:0},0).wait(1));

	// Antenna_Right
	this.instance_3 = new lib.ButterflyOrange_Antenna_Right_GRAPHIC("single",0);
	this.instance_3.setTransform(14.5,-51.2,1,1,0,0,0,-10.5,31.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_3).wait(11).to({startPosition:0},0).wait(12).to({startPosition:0},0).wait(1));

	// Antenna_Left
	this.instance_4 = new lib.ButterflyOrange_Antenna_Left_GRAPHIC("single",0);
	this.instance_4.setTransform(-17.7,-52.2,1,1,0,0,0,7.8,32.8);

	this.timeline.addTween(cjs.Tween.get(this.instance_4).wait(11).to({startPosition:0},0).wait(12).to({startPosition:0},0).wait(1));

	// Body
	this.instance_5 = new lib.ButterflyOrange_Body_GRAPHIC("single",0);
	this.instance_5.setTransform(-1.5,19);

	this.timeline.addTween(cjs.Tween.get(this.instance_5).wait(11).to({startPosition:0},0).wait(12).to({startPosition:0},0).wait(1));

	// Wing_Right
	this.instance_6 = new lib.ButterflyOrange_Wing_Right_GRAPHIC("single",0);
	this.instance_6.setTransform(0,1,1,1,0,0,0,-91,0.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_6).to({scaleX:0.25},11).to({scaleX:1},12).wait(1));

	// Wing_Left
	this.instance_7 = new lib.ButterflyOrange_Wing_Left_GRAPHIC("single",0);
	this.instance_7.setTransform(-7,0.5,1,1,0,0,0,87,0.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_7).to({scaleX:0.25},11).to({scaleX:1},12).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-181.5,-130,364,261);


(lib.ButterflyOrange_FLAP1 = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Nose
	this.instance = new lib.ButterflyOrange_Nose_GRAPHIC("single",0);
	this.instance.setTransform(-2.4,-53.6);

	this.timeline.addTween(cjs.Tween.get(this.instance).wait(5).to({startPosition:0},0).wait(6).to({startPosition:0},0).wait(1));

	// Eye_Right
	this.instance_1 = new lib.ButterflyOrange_Eye_Right_GRAPHIC("single",0);
	this.instance_1.setTransform(7.7,-49.2);

	this.timeline.addTween(cjs.Tween.get(this.instance_1).wait(5).to({startPosition:0},0).wait(6).to({startPosition:0},0).wait(1));

	// Eye_Left
	this.instance_2 = new lib.ButterflyOrange_Eye_Left_GRAPHIC("single",0);
	this.instance_2.setTransform(-10.9,-50.4);

	this.timeline.addTween(cjs.Tween.get(this.instance_2).wait(5).to({startPosition:0},0).wait(6).to({startPosition:0},0).wait(1));

	// Antenna_Right
	this.instance_3 = new lib.ButterflyOrange_Antenna_Right_GRAPHIC("single",0);
	this.instance_3.setTransform(14.5,-51.2,1,1,0,0,0,-10.5,31.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_3).wait(5).to({startPosition:0},0).wait(6).to({startPosition:0},0).wait(1));

	// Antenna_Left
	this.instance_4 = new lib.ButterflyOrange_Antenna_Left_GRAPHIC("single",0);
	this.instance_4.setTransform(-17.7,-52.2,1,1,0,0,0,7.8,32.8);

	this.timeline.addTween(cjs.Tween.get(this.instance_4).wait(5).to({startPosition:0},0).wait(6).to({startPosition:0},0).wait(1));

	// Body
	this.instance_5 = new lib.ButterflyOrange_Body_GRAPHIC("single",0);
	this.instance_5.setTransform(-1.5,19);

	this.timeline.addTween(cjs.Tween.get(this.instance_5).wait(5).to({startPosition:0},0).wait(6).to({startPosition:0},0).wait(1));

	// Wing_Right
	this.instance_6 = new lib.ButterflyOrange_Wing_Right_GRAPHIC("single",0);
	this.instance_6.setTransform(0,1,1,1,0,0,0,-91,0.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_6).to({scaleX:0.25},5).to({scaleX:1},6).wait(1));

	// Wing_Left
	this.instance_7 = new lib.ButterflyOrange_Wing_Left_GRAPHIC("single",0);
	this.instance_7.setTransform(-7,0.5,1,1,0,0,0,87,0.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_7).to({scaleX:0.25},5).to({scaleX:1},6).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-181.5,-130,364,261);


(lib.ButterflyOrange_FLAPCYCLE = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Wing_Right
	this.instance = new lib.ButterflyOrange_Wing_Right_FLAP("synched",0);
	this.instance.setTransform(0,2.5,1,1,90.3,0,0,-91,2);

	this.timeline.addTween(cjs.Tween.get(this.instance).to({y:-8,startPosition:3},3).to({y:-4.5,startPosition:6},3).to({scaleX:0.88,y:0,startPosition:10},4).to({scaleX:1,y:1.1},1).wait(1));

	// Body
	this.instance_1 = new lib.ButterflyOrange_Body_SIDE_ANIM("synched",0);
	this.instance_1.setTransform(-18,2.5,1,1,86.3);

	this.timeline.addTween(cjs.Tween.get(this.instance_1).to({rotation:95.7,y:-5.5,startPosition:6},6).to({rotation:90,y:-1.5,startPosition:11},5).wait(1));

	// Wing_Left
	this.instance_2 = new lib.ButterflyOrange_Wing_Left_FLAP("synched",6);
	this.instance_2.setTransform(0.5,-7,1,1,90,0,0,87,0.5);

	this.timeline.addTween(cjs.Tween.get(this.instance_2).to({rotation:82.3,y:-12.1,startPosition:0},6).to({rotation:90,y:-7,startPosition:5},5).wait(1));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = new cjs.Rectangle(-129.4,-66.5,261.9,252.1);


// stage content:



(lib.Homepage_Butterfly_2 = function(mode,startPosition,loop) {
	this.initialize(mode,startPosition,loop,{});

	// Layer 13
	this.instance = new lib.ButterflyOrange_FLAP2("synched",0);
	this.instance.setTransform(1505.5,257.8,0.212,0.212,0,13.5,-166.5,-18.9,-16.4);
	this.instance._off = true;

	this.instance_1 = new lib.ButterflyOrange_FLAP3("synched",0);
	this.instance_1.setTransform(1543.6,721,0.354,0.354,0,0,0,-18.9,-16.8);

	this.timeline.addTween(cjs.Tween.get({}).to({state:[]}).to({state:[{t:this.instance}]},239).to({state:[{t:this.instance}]},118).to({state:[]},1).to({state:[{t:this.instance_1}]},72).to({state:[]},190).to({state:[{t:this.instance}]},114).to({state:[{t:this.instance}]},144).to({state:[]},1).wait(173));
	this.timeline.addTween(cjs.Tween.get(this.instance).wait(239).to({_off:false},0).wait(118).to({startPosition:22},0).to({_off:true},1).wait(376).to({_off:false,regX:-18.7,regY:-16.9,scaleX:0.35,scaleY:0.35,rotation:-57.3,skewX:0,skewY:0,x:232.9,y:210.7,startPosition:0},0).to({regX:-18.9,regY:-16.8,rotation:-50.4,guide:{path:[232.8,210.7,233.9,210.6,234.9,210.6]}},144).to({_off:true},1).wait(173));

	// Layer 1
	this.instance_2 = new lib.ButterflyOrange_FLAPCYCLE("synched",0);
	this.instance_2.setTransform(-196.7,677.9,0.354,0.354,0,0,0,-18.9,-16.8);
	this.instance_2._off = true;

	this.instance_3 = new lib.ButterflyOrange_TurnONLY("synched",0);
	this.instance_3.setTransform(137.9,613.1,0.352,0.352,0,43.6,-136.4,-19,-16.9);
	this.instance_3._off = true;

	this.instance_4 = new lib.ButterflyOrange_FLAP1("synched",0);
	this.instance_4.setTransform(81,440.5,0.352,0.352,0,8.2,-178.8,-19.1,-17.1);
	this.instance_4._off = true;

	this.instance_5 = new lib.ButterflyOrange_SideTurnToFlat("synched",5,false);
	this.instance_5.setTransform(1645.1,279.9,0.248,0.248,0,23,-157,-17.9,-16.6);
	this.instance_5._off = true;

	this.timeline.addTween(cjs.Tween.get(this.instance_2).wait(119).to({_off:false},0).to({regX:-19.1,regY:-16.9,guide:{path:[-196.6,677.8,-173,711.5,-141.2,757.9,-139.5,760.4,-137.8,762.8,-136.5,764.5,-135.3,766.2,-110.8,801.1,-95.1,820.1,-73.2,846.4,-48.6,867.1,-23.6,888.2,15.8,913,82.8,955.6,138.7,979.5,175.4,995.3,206.3,1006.2,282,1040.4,353.8,1023.9], orient:'auto'},startPosition:10},22).to({regY:-16.8,scaleX:0.35,scaleY:0.35,rotation:-100,guide:{path:[353.8,1023.9,394.8,1014.5,434.5,988.5,468.2,966.5,464.7,932.5], orient:'auto'},startPosition:3},5).to({regX:-18.9,regY:-16.6,rotation:0,skewX:72.1,skewY:-107.9,guide:{path:[464.7,932.5,463.3,919.6,456.7,905,456.4,903.6,456,902.2], orient:'auto'},startPosition:4},1).to({regX:-19,regY:-16.8,skewX:14.9,skewY:-165.1,guide:{path:[456,902.2,452.9,889.7,446.4,876.6,443.7,871.5,440.6,866.2,426.7,843.4,399.4,818.8,398.6,818.2,397.9,817.5,383.5,804.7,362.7,789.2,361,788,359.2,786.7,349.1,779.2,337.5,771.2,325.4,762.7,303.5,747.9,278.4,730.9,268.3,724,230.7,698.1,205.1,677.5], orient:'auto'},startPosition:3},11).to({_off:true,regY:-16.9,scaleX:0.35,scaleY:0.35,skewX:43.6,skewY:-136.4,guide:{path:[204.9,677.5,201.9,675.1,199.1,672.8,194.5,669.1,190.2,665.4,170.7,648.7,155,632.3,146.3,623.2,138.7,614.1,138.3,613.6,137.8,613.1], orient:'auto'},startPosition:0},3).wait(23).to({_off:false,regX:-18.7,regY:-16.7,scaleX:0.23,scaleY:0.23,rotation:41.9,skewX:0,skewY:0,guide:{path:[466.6,358.1,479.1,367.5,491.5,376.9], orient:'auto'},startPosition:6},1).to({regY:-16.8,scaleX:0.23,scaleY:0.23,rotation:43.8,guide:{path:[491.4,376.9,492.1,377.4,492.7,377.8,506.6,379.6,520.4,381.4,549.8,379.6,571,359.7,576.1,355.1,579,348.7,591,323,617.2,315.5,639.3,312.1,656.7,326.8,661.6,331,667.2,334.3,671.9,337.1,676.8,339.6], orient:'auto'},startPosition:1},7).to({regX:-18.6,scaleX:0.24,scaleY:0.24,rotation:-82.9,guide:{path:[676.8,339.6,699.4,351.1,724.4,355,758,361.3,789,346.8,813.4,335.5,829.6,314.8,832.9,310.6,836,306.3,838.3,303.2,840.8,300.3], orient:'auto'},startPosition:7},6).to({regX:-19,regY:-16.9,scaleX:0.23,scaleY:0.23,rotation:7.7,guide:{path:[840.7,300.4,846.7,293.5,854.1,288.2,874.6,273.7,895.1,275.6], orient:'auto'},startPosition:9},2).to({regX:-18.6,regY:-16.7,scaleX:0.22,scaleY:0.22,rotation:47.5,guide:{path:[895.2,275.6,910.1,277,924.9,287.1,926,287.8,927.1,288.7,927.6,289.1,928,289.6,936.7,297.7,944.9,310.3], orient:'auto'},startPosition:11},2).to({regY:-16.8,scaleX:0.35,scaleY:0.35,rotation:32.3,guide:{path:[944.9,310.3,948.6,316,952.2,322.6,954.9,327.7,957.7,333.3,965.6,349.6,1039,399.7], orient:'auto'},startPosition:3},4).to({regY:-16.9,scaleX:0.27,scaleY:0.27,rotation:32.2,guide:{path:[1039,399.7,1068.4,419.8,1108.4,445.4,1127.2,457.4,1144.9,466.3], orient:'auto'},startPosition:7},4).to({regX:-18.7,regY:-17,scaleX:0.3,scaleY:0.3,rotation:-62.2,guide:{path:[1144.9,466.3,1258,523.1,1322.2,449.6,1328.7,439.7,1336.1,432.7], orient:'auto'},startPosition:2},7).to({regX:-18.3,regY:-16.9,scaleX:0.27,scaleY:0.27,rotation:20.9,guide:{path:[1336.1,432.7,1340.1,428.8,1344.5,425.8,1359.7,415.4,1379.3,415.4,1390.1,415.4,1410.6,422,1422.8,426,1451.9,436.5,1469.4,442.7,1484.1,446.9], orient:'auto'},startPosition:7},5).to({regX:-18.2,regY:-16.8,scaleX:0.27,scaleY:0.27,rotation:-71.9,guide:{path:[1484.1,446.8,1492.7,449.3,1500.3,451.1,1528.6,457.7,1552.7,457.7,1581.9,457.7,1606.7,449.7,1630.6,441.9,1648.2,427.5,1665.6,413.2,1674.8,393.8,1675.2,392.8,1675.7,391.8], orient:'auto'},startPosition:2},7).to({regY:-16.6,scaleX:0.29,scaleY:0.29,rotation:0,skewX:90.2,skewY:-89.8,guide:{path:[1675.7,391.9,1682.1,377.7,1683.7,361.9], orient:'auto'},startPosition:3},1).to({_off:true,regX:-17.9,scaleX:0.25,scaleY:0.25,skewX:23,skewY:-157,guide:{path:[1683.7,361.9,1684.2,356.7,1684.2,351.4,1684.2,334.7,1672.5,314.3,1662,296.2,1645.1,279.8], orient:'auto'},startPosition:5,loop:false},3).wait(128).to({_off:false,regX:-18.9,regY:-16.4,scaleX:0.2,scaleY:0.2,skewX:-39.6,skewY:-220.3,guide:{path:[1417.1,208.8,1367.7,200,1317.4,231.8]},startPosition:6},3).to({regX:-18.8,regY:-16.5,scaleX:0.19,scaleY:0.19,skewX:-107.4,skewY:-290.7,guide:{path:[1317.5,231.8,1317.1,232.1,1316.7,232.3,1298.1,244.2,1297.6,257.3]},startPosition:7},1).to({regY:-16.4,scaleX:0.24,scaleY:0.24,skewX:-157.9,skewY:-341,guide:{path:[1297.6,257.3,1297.1,271.8,1318.6,287.7,1335.5,300.3,1355.4,306.9,1366.6,311.3,1379.5,315.3,1395.7,320.9,1411.8,326.5]},startPosition:11},4).to({_off:true,scaleX:0.25,scaleY:0.25,skewX:19.4,skewY:-337.6,guide:{path:[1411.9,326.4,1422.8,330.2,1433.7,334,1439,336.3,1444.3,338.7]},startPosition:0},1).wait(36).to({_off:false,regX:-18.7,regY:-15.9,scaleX:0.25,scaleY:0.25,skewX:21.1,skewY:-160.4,x:1358.2,y:1046.8},0).to({regX:-19.1,regY:-15.8,scaleX:0.26,scaleY:0.26,skewX:78.9,skewY:-102.1,guide:{path:[1358.1,1046.6,1338.1,1043,1318.4,1036.7,1272.9,1022,1237.1,995.2,1235.3,993.9,1233.5,992.5,1198.7,965.5,1179,930.7,1164.6,905,1159.5,877.1]},startPosition:8},8).to({regX:-18.8,regY:-15.7,scaleX:0.23,scaleY:0.23,skewX:142.7,skewY:-38.1,guide:{path:[1159.5,877.1,1156.9,862.2,1156.9,846.7,1156.9,738,1222.8,689.9,1223.9,689.1,1225,688.3,1236.1,680.5,1249,674.4]},startPosition:2},6).to({regX:-18.4,regY:-15.9,skewX:-22.3,skewY:-21.8,guide:{path:[1249,674.5,1259,669.7,1270.2,665.9]},startPosition:3},1).to({_off:true,regX:-18.6,regY:-15.8,skewX:62.4,skewY:62.7,guide:{path:[1270.2,665.9,1299.9,655.9,1337.4,653.2]},startPosition:0,loop:false},3).wait(196).to({_off:false,regX:-18.9,regY:-16.8,scaleX:0.35,scaleY:0.35,skewX:0,skewY:0,guide:{path:[1543.6,721.1,1572.8,708.7,1633,698.1,1658.2,693.7,1680.8,691,1703,688.3,1713.5,688.3,1722.6,688.3,1731.3,688.8]},startPosition:7},7).to({rotation:57.7,guide:{path:[1731.3,688.8,1772.4,691.4,1803.6,706.3,1836.1,721.9,1856.9,750.5,1869.8,768.4,1877.7,790.6]},startPosition:2},7).to({regX:-19.1,regY:-16.9,rotation:0,skewX:-101.6,skewY:78.4,guide:{path:[1877.7,790.6,1880.3,797.9,1882.3,805.7,1883.7,811.2,1884.9,816.9]},startPosition:3},1).to({regX:-18.9,regY:-17.1,scaleX:0.35,scaleY:0.35,skewX:-32.2,skewY:147.8,guide:{path:[1884.9,816.9,1888.8,836.4,1888.8,856.5,1888.8,911,1853.5,951.6,1838.8,968.5,1818.3,982.7]},startPosition:10},7).to({_off:true,regX:-19.2,regY:-16.9,scaleX:0.35,scaleY:0.35,skewX:9.3,skewY:189.3,guide:{path:[1818.3,982.7,1795.1,998.7,1764.6,1011.3,1714.3,1032.1,1647.5,1042.4,1629.5,1045.2,1611.1,1047.1]},startPosition:0},8).wait(52).to({_off:false,regX:-19.3,scaleX:0.35,scaleY:0.35,skewX:28.8,skewY:208.8,guide:{path:[489.1,766.7,484.1,753.4,478.8,740.1,471.6,714,455.2,693.3,446.8,682.7,434.6,676.7,422,670.5,408.7,666.2]},startPosition:3},5).to({regX:-19.4,rotation:-29.1,skewX:0,skewY:360,guide:{path:[408.8,666.2,402.7,664.3,396.5,662.7,391.4,661.4,386.7,658.2,384.3,656.6,382.1,654.8,367.8,643.5,362.7,627.7,354.2,595.2,386,579.3,388.4,578.1,390.7,576.7,394.2,574.7,398.1,572.8]},startPosition:8},5).to({regY:-16.8,rotation:-86.5,guide:{path:[398.1,572.7,400.4,571.6,402.8,570.4,430.8,559.8,453.2,539.6,459,534.4,464.6,528.7,480.8,512.3,484.3,490.5,485.2,484.6,485,478.7]},startPosition:1},5).to({regY:-16.6,rotation:0,skewX:52.8,skewY:232.8,guide:{path:[485,478.7,484.9,476.1,484.6,473.5,483.9,467.4,482.2,461.6,480.8,456.9,478.8,452.4]},startPosition:2},1).to({regX:-19.3,skewX:21.5,skewY:201.5,guide:{path:[478.8,452.5,477.8,450.1,476.5,447.8,466.8,429.8,445.9,417.7,434.8,411.8,420.4,407.7,403.1,402.8,388.5,398.1]},startPosition:6},4).to({regX:-19.2,scaleX:0.35,scaleY:0.35,skewX:82.5,skewY:262.5,guide:{path:[388.4,398.1,382.4,396.2,376.9,394.3,340.3,382.1,323.4,371.8,304.9,360.6,294.9,313]},startPosition:11},5).to({_off:true,rotation:-66,skewX:0,skewY:360,guide:{path:[295,313,292.4,300.7,290.4,285.9]},startPosition:0,loop:false},1).wait(151).to({_off:false,regX:-18.6,regY:-16.8,rotation:0,skewX:-105.1,skewY:434.9,guide:{path:[239.7,199.5,203.6,217.5,193.9,222.2,129.8,253,165.9,305.9]},startPosition:7},6).to({regY:-16.9,skewX:-135.5,skewY:404.5,guide:{path:[165.9,305.9,174.2,318.1,187.8,331.5,204,347.3,213.2,360.1]},startPosition:10},3).to({regY:-16.8,skewX:-96.5,skewY:443.5,guide:{path:[213.2,360.1,234.1,389.2,219.3,403]},startPosition:0},2).to({regX:-18.8,regY:-16.6,skewX:-5.8,skewY:534.2,guide:{path:[219.2,402.9,211.4,410.2,193.4,413.2,174.4,417.2,154.3,418.4,151.6,418.7,148.8,418.9,148.4,419,147.9,419.1]},startPosition:3},3).to({regX:-18.9,skewX:-94.7,skewY:445.3,guide:{path:[147.9,419.1,134.4,421.3,123.6,425.8,97.7,435.9,87.7,461,79.3,481.8,86.8,505.6]},startPosition:8},5).to({regX:-18.8,skewX:-182.3,skewY:357.7,guide:{path:[87,505.6,88.3,509.9,90,514.3,102.3,543.3,131.2,562.1,146.9,572.3,164,577.7,170.1,579.6,176.5,580.8]},startPosition:1},5).to({regX:-18.9,skewX:-173.1,skewY:366.9,guide:{path:[176.6,580.8,205.6,586.6,238.6,579.5,317.1,550.8,393.6,575.3]},startPosition:10},9).to({regY:-16.5,skewX:-88.6,skewY:451.4,guide:{path:[393.6,575.2,399.1,577,404.6,579,437.2,591,461.8,615.5,466.2,620,470.3,624.8,524.7,695.5,527.5,785.5,528,799.7,526.8,813.3]}},12).to({regY:-16.6,skewX:-15.9,skewY:524.1,guide:{path:[526.8,813.3,520.6,883.1,470.3,937.2,453.5,956.9,427.8,981.4]},startPosition:6},8).to({regY:-16.8,scaleX:0.35,scaleY:0.35,skewX:-44.2,skewY:495.8,guide:{path:[427.8,981.4,336.7,1068.4,134.1,1216.4]},startPosition:8},15).wait(105));
	this.timeline.addTween(cjs.Tween.get(this.instance_3).wait(158).to({_off:false},3).to({_off:true,regX:-19.1,regY:-17.1,skewX:8.2,skewY:-178.8,guide:{path:[137.8,613,110.3,579.8,96,544.8,79.8,505.1,79.8,461.5,79.8,450.9,80.9,440.4], orient:'auto'}},6).wait(11).to({_off:false,regX:-18.9,regY:-16.9,scaleX:0.35,scaleY:0.35,rotation:120.9,skewX:0,skewY:0,guide:{path:[318.5,246.7,323.5,247.9,328.5,249.4,337.6,252.1,346.9,256,347.5,256.2,348.1,256.5], orient:'auto'},startPosition:6,loop:false},1).to({regY:-17.1,scaleX:0.25,scaleY:0.25,rotation:0,skewX:84.1,skewY:54,guide:{path:[348.1,256.5,359.8,262,375.2,280.5,391.8,299.8,413.2,317.4,439.9,337.7,466.6,358], orient:'auto'},startPosition:11},5).to({_off:true,regX:-18.7,regY:-16.7,scaleX:0.23,scaleY:0.23,rotation:41.9,skewX:0,skewY:0,guide:{path:[466.6,358.1,479.1,367.5,491.5,376.9], orient:'auto'},startPosition:6},1).wait(173).to({_off:false,regX:-18.9,regY:-16.4,scaleX:0.21,scaleY:0.21,rotation:0,skewX:13.5,skewY:-166.5,x:1505.7,y:257.5,loop:false},0).to({regY:-16.6,scaleX:0.28,scaleY:0.28,skewX:-51,skewY:-231.3,guide:{path:[1505.6,257.4,1461.7,216.6,1417.2,208.8]},startPosition:9},3).to({_off:true,regY:-16.4,scaleX:0.2,scaleY:0.2,skewX:-39.6,skewY:-220.3,guide:{path:[1417.1,208.8,1367.7,200,1317.4,231.8]},startPosition:6},3).wait(5).to({_off:false,regX:-18.8,scaleX:0.25,scaleY:0.25,skewX:19.4,skewY:-337.6,guide:{path:[1411.9,326.4,1422.8,330.2,1433.7,334,1439,336.3,1444.3,338.7]},startPosition:0},1).to({regY:-16.1,scaleX:0.31,scaleY:0.31,skewX:127.9,skewY:-229.3,guide:{path:[1444.2,338.7,1474.1,352.4,1502.4,369.7,1510.4,374.7,1518.6,379.6,1522.6,382,1526.9,384.5,1561.3,405.1,1579.9,415.4,1602.5,428,1623.3,440.9]},startPosition:6},6).to({_off:true,regX:-18.9,scaleX:0.32,scaleY:0.32,skewX:127.1,skewY:-230.2,guide:{path:[1623.3,440.8,1638.3,450.1,1652.2,459.6]},startPosition:0},1).wait(18).to({_off:false,regX:-18.8,regY:-15.8,scaleX:0.35,scaleY:0.35,skewX:256.1,skewY:-102.2,guide:{path:[1722.7,960.2,1661.3,1002.2,1583.8,1025.8,1574.3,1028.7,1564.7,1031.3]},startPosition:6,loop:false},5).to({regX:-18.6,regY:-15.6,scaleX:0.25,scaleY:0.25,skewX:198.9,skewY:-159.4,guide:{path:[1564.7,1031.3,1490,1051.3,1410.3,1051.3,1400.5,1051.3,1390.8,1050.6]},startPosition:11},5).to({_off:true},1).wait(15).to({_off:false,regY:-15.8,scaleX:0.23,scaleY:0.23,skewX:62.4,skewY:-297.3,guide:{path:[1270.2,665.9,1299.9,655.9,1337.4,653.2]},startPosition:0},3).to({regX:-18.5,regY:-15.7,scaleX:0.35,scaleY:0.35,skewX:98.3,skewY:-261.5,guide:{path:[1337.4,653.2,1351.3,652.2,1366.3,652.2,1399.5,652.2,1423.3,655.5,1441.9,658.1,1457.8,663]},startPosition:3},3).to({regX:-18.6,regY:-15.8,scaleX:0.35,scaleY:0.35,skewX:22.9,skewY:-336.8,guide:{path:[1457.8,663,1463.6,664.8,1469.1,667,1479.6,671.1,1489.7,676.8]},startPosition:4},1).to({scaleX:0.35,scaleY:0.35,rotation:9.1,skewX:0,skewY:-360,guide:{path:[1489.6,676.7,1499.2,682.2,1508.4,689.1,1513.2,692.7,1518.1,696.8]},startPosition:5},1).to({regX:-18.9,regY:-16.8,scaleX:0.35,scaleY:0.35,rotation:0,guide:{path:[1518.1,696.8,1530.4,707.3,1543.2,721.2]},mode:"single",startPosition:6},1).to({_off:true},1).wait(189).to({_off:false,y:721.1,mode:"synched",loop:false},0).to({_off:true,guide:{path:[1543.6,721.1,1572.8,708.7,1633,698.1,1658.2,693.7,1680.8,691,1703,688.3,1713.5,688.3,1722.6,688.3,1731.3,688.8]},startPosition:7},7).wait(15).to({_off:false,regX:-19.2,regY:-16.9,skewX:9.3,skewY:-530.7,guide:{path:[1818.3,982.7,1795.1,998.7,1764.6,1011.3,1714.3,1032.1,1647.5,1042.4,1629.5,1045.2,1611.1,1047.1]},startPosition:0},8).to({_off:true,regX:-19.3,scaleX:0.35,scaleY:0.35,skewX:-44.6,skewY:-584.6,guide:{path:[1611.1,1047.1,1570.4,1051.3,1527.8,1051.3,1518.5,1051.3,1509.5,1050.9,1478,1050,1448.5,1046.1]}},6).wait(38).to({_off:false,regY:-17.7,scaleX:0.35,scaleY:0.35,skewX:-23.1,skewY:-563.1,guide:{path:[563.5,971.4,535.9,901.1,511.2,828.4,500.7,797.4,489.1,766.7]},startPosition:6},8).to({_off:true,regY:-16.9,scaleX:0.35,scaleY:0.35,skewX:28.8,skewY:-511.2,guide:{path:[489.1,766.7,484.1,753.4,478.8,740.1,471.6,714,455.2,693.3,446.8,682.7,434.6,676.7,422,670.5,408.7,666.2]},startPosition:3},5).wait(20).to({_off:false,regX:-19.2,regY:-16.6,scaleX:0.35,scaleY:0.35,rotation:-66,skewX:0,skewY:-360,guide:{path:[295,313,292.4,300.7,290.4,285.9]},startPosition:0,loop:false},1).to({regX:-18.7,regY:-16.9,scaleX:0.35,scaleY:0.35,rotation:-57.3,guide:{path:[290.4,285.9,289.4,278.3,288.5,270.1,281.8,207.3,232.9,210.7]},mode:"single",startPosition:6},6).to({_off:true},1).wait(144).to({_off:false,regX:-18.8,regY:-17.2,rotation:0,skewX:-53.5,skewY:-233.5,x:239.8,y:199.7,mode:"synched",loop:false},0).to({_off:true,regX:-18.6,regY:-16.8,scaleX:0.35,scaleY:0.35,skewX:-105.1,skewY:-285.1,guide:{path:[239.7,199.5,203.6,217.5,193.9,222.2,129.8,253,165.9,305.9]},startPosition:7},6).wait(167));
	this.timeline.addTween(cjs.Tween.get(this.instance_4).wait(161).to({_off:false},6).to({regX:-18.9,regY:-16.8,skewX:40.9,skewY:-141.9,guide:{path:[81.1,440.5,84.2,410.5,96.2,381,111.8,342.2,140.1,311.1,165.9,282.9,197.8,265.8], orient:'auto'},startPosition:7},7).to({regX:-19,regY:-16.6,scaleX:0.35,scaleY:0.35,skewX:102.8,skewY:-77.7,guide:{path:[197.8,265.9,201.8,263.7,205.8,261.8,244.7,243.1,286.5,243.1,302.3,243.1,318.4,246.8], orient:'auto'},startPosition:11},4).to({_off:true,regX:-18.9,regY:-16.9,rotation:120.9,skewX:0,skewY:0,guide:{path:[318.5,246.7,323.5,247.9,328.5,249.4,337.6,252.1,346.9,256,347.5,256.2,348.1,256.5], orient:'auto'},startPosition:6,loop:false},1).wait(197).to({_off:false,regY:-16.1,scaleX:0.32,scaleY:0.32,rotation:0,skewX:127.1,skewY:129.8,guide:{path:[1623.3,440.8,1638.3,450.1,1652.2,459.6]},startPosition:0},1).to({scaleX:0.33,scaleY:0.33,skewX:131.9,skewY:134.5,guide:{path:[1652.1,459.6,1732,513.7,1780.6,573.6]},startPosition:5},5).to({regX:-19,regY:-16.4,scaleX:0.34,scaleY:0.34,skewX:178.8,skewY:181,guide:{path:[1780.7,573.7,1784.2,578,1787.5,582.3,1852.8,667.3,1852.8,759.3,1852.8,761.5,1852.7,763.6]},startPosition:11},6).to({regX:-19.1,regY:-16.5,scaleX:0.35,scaleY:0.35,skewX:242.7,skewY:244.7,guide:{path:[1852.7,763.6,1851.3,814.7,1818.4,865.5,1784.7,917.4,1724.3,959.1,1723.5,959.7,1722.7,960.3]},startPosition:6},7).to({_off:true,regX:-18.8,regY:-15.8,scaleX:0.35,scaleY:0.35,skewX:256.1,skewY:257.8,guide:{path:[1722.7,960.2,1661.3,1002.2,1583.8,1025.8,1574.3,1028.7,1564.7,1031.3]},loop:false},5).wait(250).to({_off:false,regX:-19.3,regY:-16.9,scaleX:0.35,scaleY:0.35,skewX:315.4,skewY:135.4,guide:{path:[1611.1,1047.1,1570.4,1051.3,1527.8,1051.3,1518.5,1051.3,1509.5,1050.9,1478,1050,1448.5,1046.1]},startPosition:0},6).to({regX:-19.4,scaleX:0.35,scaleY:0.35,skewX:305.4,skewY:125.4,guide:{path:[1448.5,1046.1,1394.8,1038.8,1348,1021.2,1341.1,1018.6,1334.4,1015.8,1301.4,1002,1270.8,982.4]},startPosition:7},7).to({regX:-19.3,skewX:330.7,skewY:150.7,guide:{path:[1270.8,982.5,1251.2,969.9,1232.5,954.9,1213.8,940,1192.4,919.5,1170,897.7,1160.4,888.6,1144.3,873.4,1134,866.8,1129.7,864,1125.7,862.2]},startPosition:2},7).to({regY:-17,scaleX:0.35,scaleY:0.35,skewX:228.4,skewY:48.4,guide:{path:[1125.7,862.1,1117.6,858.4,1110.5,858.4,1104.3,858.4,1098.5,859.1,1088.3,863.2,1077.3,872.1,1064.2,882.7,1041.4,908.4,1039.3,910.7,1037.4,912.9]},startPosition:6},4).to({regX:-19.2,regY:-17.2,skewX:304.1,skewY:124.1,guide:{path:[1037.4,912.9,1016.3,936.4,1006.9,944.6,988.1,961.1,973.4,961.1,962.3,961.1,948.8,956.8]},startPosition:10},4).to({regX:-19.4,regY:-17.3,scaleX:0.35,scaleY:0.35,skewX:321.6,skewY:141.6,guide:{path:[948.8,956.8,946.9,956.2,944.9,955.5,934.1,951.7,916.7,943.1,896.5,933.1,890.7,930.7,881.4,926.8,874.5,925.6]},startPosition:1},3).to({regX:-19.3,skewX:240.3,skewY:60.3,guide:{path:[874.5,925.5,871.3,925,868.6,925,850.8,925,821.6,938.9]},startPosition:3},2).to({regX:-19.2,regY:-17.4,scaleX:0.35,scaleY:0.35,skewX:238.4,skewY:58.4,guide:{path:[821.6,938.9,820.5,939.5,819.3,940.1,801.1,949,760.8,973.3,754.9,976.9,749.5,980.1]},startPosition:6},3).to({skewX:329.5,skewY:149.5,guide:{path:[749.5,980.1,718,998.7,701.9,1006.6,670.7,1021.7,651.6,1021.7,641.6,1021.7,632.8,1020.2,588.2,1005.4,567.8,982.1,565.7,976.7,563.6,971.3]},startPosition:2},8).to({_off:true,regX:-19.3,regY:-17.7,skewX:336.9,skewY:156.9,guide:{path:[563.5,971.4,535.9,901.1,511.2,828.4,500.7,797.4,489.1,766.7]},startPosition:6},8).wait(350));
	this.timeline.addTween(cjs.Tween.get(this.instance_5).wait(230).to({_off:false},3).to({regX:-18.1,scaleX:0.27,scaleY:0.27,skewX:17.4,skewY:-162.6,guide:{path:[1645,279.8,1643.6,278.4,1642.2,277.1,1622.6,259,1600.6,248.5,1589.6,243.2,1579.1,240.3], orient:'auto'},startPosition:8},3).to({regX:-17.9,regY:-16.9,scaleX:0.23,scaleY:0.23,skewX:-20.4,skewY:-200.4,guide:{path:[1579,240.3,1566.7,237,1554.9,237,1554.2,237,1553.4,237], orient:'auto'},startPosition:9},1).to({regX:-18.9,regY:-16.3,scaleX:0.21,scaleY:0.21,skewX:13.5,skewY:-166.5,guide:{path:[1553.5,237.1,1545.9,237.7,1531.4,244.3,1526.8,246.4,1521.4,249.2,1513.3,253.3,1505.3,257.5], orient:'auto'},startPosition:11},2).to({_off:true},1).wait(812));

}).prototype = p = new cjs.MovieClip();
p.nominalBounds = null;

})(lib = lib||{}, images = images||{}, createjs = createjs||{}, ss = ss||{});
var lib, images, createjs, ss;
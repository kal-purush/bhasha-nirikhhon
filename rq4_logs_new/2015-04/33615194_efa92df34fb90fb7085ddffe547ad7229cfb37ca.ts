
var PI = Math.PI;
var TWO_PI = 2*Math.PI;
var Random = Math.random;

// CONSTANTS
var MAX_R = 32;
var MAX_POWER = 150;
var MAX_SPEED = 250;
var FIRE = 5;
var BRAKE = 0.9;

var SMOKE_COLOR = ['rgba(37,37,37,','rgba(21,21,21,'];
var SMOKE_F = 0.99;
var SMOKE_MAX = 5;

var GUE_F = 0.98;
var GUE_MAX = 30;
var GUE_COLOR = 'rgba(0,255,0,';

var CHECKPOINT_MAX = 9.99;
var CHECKPOINT_R = 30;

var SPARK_T = 3;
var SPARK_F = 0.99;
var SPARK_SIZE = 1;

var POINTS_MAX = 3;

/*
 * Utils
 */

var collides = function( ship, p, r ){
	var xx = ship.p.x - p.x;
	var yy = ship.p.y - p.y;
	var rr = ship.radius + r;

	if( xx*xx + yy*yy > rr*rr )
		return false;

	var p0 = ship.rot(5,-5);
	var p1 = ship.rot(-5,5);
	var p2 = ship.rot(15,0);

	return inters( p0, p1, p, r ) ||
			inters( p1, p2, p, r ) ||
			inters( p2, p0, p, r ) ;
}

//XXX FIXME
//http://www.gamedev.net/community/forums/topic.asp?topic_id=304578
var inters = function(p0,p1,c,cw){
    var x0 = c.x;
	var y0 = c.y;
	var x1 = p0.x;
	var y1 = p0.y;
	var x2 = p1.x;
	var y2 = p1.y;

	var n = Math.abs( (x2-x1)*(y1-y0) - (x1-x0)*(y2-y1) );
	var d = Math.sqrt( (x2-x1)*(x2-x1) + (y2-y1)*(y2-y1) );
	var dist = n/d;
	if( dist > cw )
		return false;

	var d1 = Math.sqrt( (x0-x1)*(x0-x1) + (y0-y1)*(y0-y1) );
	if( (d1-cw) > d )
		return false;

	var d2 = Math.sqrt( Math.pow(x0-x2,2) + Math.pow(y0-y2,2));
	if( (d2-cw) > d )
		return false;

	return true;
}

var fix = function(str,length){
	var tmp = str;
	while (tmp.length < length) {
    	tmp = ' ' + tmp;
	}
	return tmp;
};

var drawMissile = function(ctx){
	ctx.beginPath();
		ctx.moveTo(-4, -4);
		ctx.lineTo(-4, 4);
		ctx.lineTo(8, 4);
		ctx.lineTo(12, 0);
		ctx.lineTo(8, -4);
		ctx.lineJoin = 'miter';
	ctx.closePath();
};

var drawTriangle = function(ctx){
	ctx.beginPath();
		ctx.moveTo(-5, -5);
		ctx.lineTo(-5, 5);
		ctx.lineTo(15, 0);
		ctx.lineJoin = 'miter';
	ctx.closePath();
};

/*
 * Actors
 */

var Ship = function(x,y){

    var power = null;
    var p = { x:x, y:y };
	var v = { x:0, y:0 };
	var r = 0;

	var points = 0;
	var timer = 0;
	var max = 0;

	this.p = p;
	this.radius = 15;

	this.rot = function(x,y){
		var cos_angle = Math.cos( this.angle() );
    	var sin_angle = Math.sin( this.angle() );
		return { x : (x*cos_angle - y*sin_angle)+p.x, y : (x*sin_angle + y*cos_angle)+p.y };
	}

	this.points = function(p){
		points += p;
		timer += 3;
	}

	this.getVel = function(){
		return Math.sqrt( Math.pow(v.x,2) + Math.pow(v.y,2) );
	};

	this.setVel = function(pow){
		v.x = pow*Math.cos( this.angle() );
        v.y = pow*Math.sin( this.angle() );
   	};

   	this.addVel = function(pow){
		v.x += pow*Math.cos( this.angle() );
        v.y += pow*Math.sin( this.angle() );
   	};

    this.angle = function(){
		return TWO_PI/MAX_R*r;
	};

    this.draw = function(ctx){
        ctx.save();

		ctx.translate( p.x, p.y );
		ctx.rotate( this.angle() );

		//if engines on
        if( up && !down ){
        	ctx.save();
	        	ctx.beginPath();

				ctx.moveTo(-5, -4);
				ctx.lineTo(-5, 4);
				ctx.lineTo(-14-(Random()*2), 0);
				ctx.closePath();

				ctx.fillStyle = "rgba(256, 236, 80, "+(Random()*0.5+0.5)+")";
				ctx.fill();
            ctx.restore();

            var xx = p.x-(Math.cos(this.angle())*17);
            var yy = p.y-(Math.sin(this.angle())*17);
            actors.push( new Smoke(xx,yy,Math.cos(this.angle()),Math.sin(this.angle())) );

        }

		drawTriangle(ctx);

		ctx.fillStyle = '#ff2020';
		ctx.fill();

		if( up && down ){
			ctx.lineWidth = 4;
			ctx.lineJoin = 'round';
			ctx.strokeStyle = "rgba(80, 236, 256, "+(Random()*0.6+0.4)+")";
		}
		else{
			if( down ){
				ctx.lineWidth = 3;
				ctx.strokeStyle = 'rgba(255,20,20,0.2)';
			}
			else {
				ctx.lineWidth = 2;
				ctx.strokeStyle = '#ffbbbb';
			}
		}
		ctx.stroke();

		ctx.restore();
    };

    this.left  = function() { r = (r-1) % MAX_R; };
    this.right = function() { r = (r+1) % MAX_R; };

    this.fire = function() {
        v.x += FIRE*Math.cos( this.angle() );
        v.y += FIRE*Math.sin( this.angle() );

        // when reaches limit starts to friction
        if( this.getVel() > MAX_SPEED ){
        	this.slowdown(GUE_F);
        }
    };

    this.brake = function(){
        v.x *= BRAKE;
        v.y *= BRAKE;
    };

    this.slowdown = function(friction){
        v.x *= friction;
        v.y *= friction;
    };

    this.powerBrake = function(isOn){
    	if( !isOn && power !== null ){
    		// apply boost
        	//this.setVel( power );
        	this.addVel( power );
        	power = null;
        	return;
    	}

    	if( isOn ){
	    	if( power === null ){
	    		// power brake speed to remember
	    		power = 0; //this.getVel();
	    		//power = Math.min( power, MAX_POWER );
	    	}
	    	var tmp = this.getVel();
	        this.brake();
	        tmp -= this.getVel();
	        power += tmp;
       	}
    };

    this.tick = function(t,H,W){
        p.x += v.x*t;
	    p.y += v.y*t;

	    timer -= tick;
	    if( timer <= 0 ){
	    	if( points > 0 ){
	    		if( max >= points )
	    			actors.push( new Points(W/2,H/2,'SCORE RESET',40) );
	    		else
	    			actors.push( new Points(W/2,H/2,'NEW RECORD!!',50) );
		    	max = Math.max( max, points );
		    	points = 0;
	    	}
	    	timer = 0;
	    }

	    this.bounds(H,W);
    };

    this.bounds = function(H,W){
		if( p.x < 0 ) p.x = W;
    	if( p.x > W ) p.x = 0;

    	if( p.y < 0 ) p.y = H;
    	if( p.y > H ) p.y = 0;
    };

	this.dead = function(){
		return false;
	};

	this.collision = function(s){
		// never collides with self
	};

    this.toString = function(){
    	var score = 'score: '+fix(points.toFixed(1),5)+' '
    		+(timer>0 ?'timeout: '+timer.toFixed(1)+'s':'(max: '+max.toFixed(1)+')');
    	if(!debug)
    		return score;

    	var xx = (p.x).toFixed(1);
    	var yy = (p.y).toFixed(1);
    	var vv = this.getVel().toFixed(1);
		var pp = power !== null ? '('+power.toFixed(1)+')':'';
    	return score+' pos=('+fix(xx,6)+
    		', '+fix(yy,6)+') vel='+fix(vv,6)+' '+pp;
    };

};

var CheckPoint = function(){
	var t = CHECKPOINT_MAX;
	var x = Random()*(W-CHECKPOINT_R*2)+CHECKPOINT_R;
	var y = Random()*(H-CHECKPOINT_R*2)+CHECKPOINT_R;
	var p = { x : x, y : y };
	var r = (1-t/CHECKPOINT_MAX)*CHECKPOINT_R+2;

	this.dead = function() {
		return t <= 0;
	}

	this.draw = function(ctx) {
		var df = t/CHECKPOINT_MAX; // from 1 to 0

        ctx.save();
	        ctx.beginPath();
			ctx.arc(x, y, r, 0, TWO_PI, false);

			ctx.lineWidth = 3*(df)+1;
			ctx.strokeStyle = 'rgba('+Math.round(255*(1-df))+','+Math.round(255*df)
				+',128,'+(df<0.5 ? (1-df) : df )+')';
			//'rgba(0,255,0,'+(1-df)+')';
			ctx.fillStyle = 'rgba( 0,'+Math.round(255*(1-df))
				+','+Math.round(255*df)+','+(df<0.5 ? (1-df) : df )+')';
			ctx.fill();
			ctx.stroke();

			if( debug ){
				ctx.fillStyle = (df < 0.5 ? 'yellow' : 'white');
				var text = t.toFixed(1);
				ctx.fillText( text,
						x-ctx.measureText(text).width/2,
						y+(FONT_H*1.5)/2 );
			}
		ctx.restore();

    };

    this.tick = function(time){
		if( this.dead() ) // already dead
			return;

        t -= time;
        r = (1-t/CHECKPOINT_MAX)*CHECKPOINT_R+2;

        if( this.dead() ){ // just died
        	actors.push( new CheckPoint() );

        	var gues = Random()*4+2;
        	while( gues-- > 0 )
        		actors.push( new Gue(x,y) );
        }
    };

    this.collision = function(s){
		if( collides( s, p, r ) ){
			var val = t*10;
			s.points( val );
			t = 0; // dies

			var sp = Random()*10+10;
        	while( sp-- > 0 )
				actors.push( new Spark(x,y,Random()*TWO_PI) );

			actors.push( new CheckPoint() );
			actors.push( new Points(x,y,('+'+val.toFixed(1)+'!')) );
		}
	};

};


var Smoke = function(x,y,vx,vy){
    var c = SMOKE_COLOR[Math.floor(Random()*SMOKE_COLOR.length)];

	var p = { x: x+((Random()*5)-2), y: y+((Random()*5)-2) };
	var v = { x: (Random()*10+5)*vx, y: (Random()*10+5)*vy };
    var t = SMOKE_MAX;

    this.dead = function() {
		return t <= 0;
	}

    this.draw = function(ctx) {
    	var df = (t/SMOKE_MAX);
        var size = 15-12*df;

        ctx.save();
	        ctx.beginPath();
			ctx.arc( p.x, p.y, size, 0, TWO_PI, false);
			ctx.fillStyle = c+(df+0.01)+')';
			ctx.fill();
			ctx.closePath();
		ctx.restore();
    };

    this.tick = function(time){
        t -= time;
        p.x += v.x*time;
	    p.y += v.y*time;
	    v.x *= SMOKE_F;
	    v.y *= SMOKE_F;
    };

    this.collision = function(s){
		// never collides
	};

};

var Gue = function(x,y){
	var angle = TWO_PI*Random();
    var size = Random()*20+30;
    var speed = Random()*20;

	var p = { x: x, y: y };
	var v = { x: Math.cos(angle)*speed, y: Math.sin(angle)*speed };
    var t = GUE_MAX;
    var s = size-10*(t/GUE_MAX);

    this.dead = function() {
		return t <= 0;
	}

    this.draw = function(ctx) {
    	var df = t/GUE_MAX;
        ctx.save();
	        ctx.beginPath();
			ctx.arc( p.x, p.y, s, 0, TWO_PI, false );
			ctx.fillStyle = GUE_COLOR+df+')';
			ctx.fill();
			ctx.closePath();
		ctx.restore();
    };

    this.tick = function(time){
        t -= time;
        s = size-10*(t/GUE_MAX);
        p.x += v.x*time;
	    p.y += v.y*time;
	    v.x *= GUE_F;
	    v.y *= GUE_F;
    };


	this.collision = function(ship) {
		if( collides( ship, p, s ) ){
			ship.slowdown( 0.8 + (1-0.8)*(1-t/GUE_MAX) );
		}
	};

};

var Points = function(x,y,val,s ? : any ){
	var p = { x: x, y: y };
    var t = POINTS_MAX;
    if ( s === undefined )
    	s = FONT_H;

    this.dead = function() {
		return t <= 0;
	}

    this.draw = function(ctx) {
        var df = (t/POINTS_MAX);

        ctx.save();
        	ctx.fillStyle = (Random() < 0.5 ? 'rgba(255,255,0' : 'rgba(255,255,255')+','+ (df+0.1) +')';
			var text = val;
			ctx.font = s+'pt testFont';
			ctx.fillText( text,
				x-ctx.measureText(text).width/2,
				y+(FONT_H*1.5)/2 );
		ctx.restore();
    };

    this.tick = function(time){
        t -= time;
    };

	this.collision = function(s){
		// never collides
	};
};

var Spark = function(x,y,angle){
	var p = { x: x+((Random()*5)-2), y: y+((Random()*5)-2) };
	var v = { x: Math.cos(angle)*12, y: Math.sin(angle)*12 };
    var t = SPARK_T;

    this.dead = function() {
		return t <= 0;
	}

    this.draw = function(ctx) {
        var df = (t/SPARK_T);

        ctx.save();
	        ctx.beginPath();
			ctx.arc( p.x, p.y, SPARK_SIZE, 0,TWO_PI, false );
			ctx.closePath();

			ctx.lineWidth = 2;
			ctx.strokeStyle = 'rgba(255,255,255,0.2)';
			ctx.fillStyle = 'rgba(255,255,0,'+(df+0.1)+')';
			ctx.fill();
			ctx.stroke();
		ctx.restore();
    };

    this.tick = function(time){
        t -= time;
        p.x += v.x*time;
	    p.y += v.y*time;
	    v.x *= SPARK_F;
	    v.y *= SPARK_F;
    };

	this.collision = function(s){
		// never collides
	};
};
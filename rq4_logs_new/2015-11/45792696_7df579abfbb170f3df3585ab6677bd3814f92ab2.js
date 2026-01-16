angular.module('WebGen').factory(
	"Line", function () {
		function Line() {
			this.points = [ 0, 0, 0, 0, 0, 0, 0, 0 ];
		}

		Line.prototype = {
			points: function() { return this.points; }
		};

		return ( Line );
	}
);

angular.module('WebGen').factory(
	"Target", ['Line', function (Line) {

		function Target(g) {
			this.goal = g;
			this.progress = 0;
			this.current = 0;
			this.points = new Line();
		}

		Target.prototype = {
			goal: function() {return this.goal;},
			progress: function() {return this.progress;},
			points: function() {return this.points;},
			current: function() {return this.current;},
			update: function() {this.progress += 1;},
			recordCurrent: function() {
				this.points[this.current] = this.progress/this.goal;
				this.current += 1;
			}
		};

		return ( Target );
	}]
);
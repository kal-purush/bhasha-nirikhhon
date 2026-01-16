/// <reference path="./node_modules/@types/node/index.d.ts"/>
/// <reference path="./node_modules/@types/jasmine/index.d.ts"/>
/// <reference path="./type_definitions/model.d.ts" />
var Model = require("./model");
var UI = require("./ui");
function string_format(format, ...args) {
    return format.replace(/{(\d+)}/g, function (match, number) {
        return typeof args[number] != 'undefined'
            ? args[number]
            : match;
    });
}
function expect_true(condition, message = "Expect true failed") {
    if (!condition) {
        console.assert(false, message);
    }
}
function expect_eq_str(expected, actual) {
    if (expected !== actual) {
        expect_true(false, string_format("\nexpected: [{0}] \nactual: [{1}]", expected, actual));
    }
}
function expect_eq_nbr(expected, actual, error = 0.01) {
    var delta = Math.abs(expected - actual);
    if (delta > error) {
        expect_true(false, string_format("\nexpected: [{0}] \nactual: [{1}]", JSON.stringify(expected), JSON.stringify(actual)));
    }
}
function createSimpleInterval(if_value, secs) {
    var i = new Model.Intensity(if_value, if_value);
    var dur = new Model.Duration(Model.TimeUnit.Seconds, secs, secs, -1 /* distance */);
    return new Model.SimpleInterval("", i, dur);
}
// TODO: Remove res
var res = null;
var mocha = require('mocha');
const assert = require('assert');
var intensity_100_pct = new Model.Intensity(1, 1);
var up = new Model.UserProfile(310, "6:00min/mi", "1:25/100yards");
up.setEfficiencyFactor(2);
var of_swim = new Model.ObjectFactory(up, Model.SportType.Swim);
var of_bike = new Model.ObjectFactory(up, Model.SportType.Bike);
var of_run = new Model.ObjectFactory(up, Model.SportType.Run);
var of_other = new Model.ObjectFactory(up, Model.SportType.Other);
describe('DistanceUnitHelper', function () {
    it('convert miles to kilometers', function () {
        let res = Model.DistanceUnitHelper.convertTo(1000, Model.DistanceUnit.Miles, Model.DistanceUnit.Kilometers);
        expect_eq_nbr(1609.344, res);
    });
});
describe('TimeUnitHelper', function () {
    it('convert hours to seconds', function () {
        let res = Model.TimeUnitHelper.convertTo(1, Model.TimeUnit.Hours, Model.TimeUnit.Seconds);
        expect_eq_nbr(3600, res);
    });
});
describe('Intensity', function () {
    it('IF Average', function () {
        var i90 = new Model.Intensity(0.90, 0.90);
        var i80 = new Model.Intensity(0.80, 0.80);
        res = Model.Intensity.combine([i90, i80], [1, 1]).getValue();
        expect_eq_nbr(0.85, res);
    });
    it('hr conversion', function () {
        var hr_visitor = new Model.WorkoutTextVisitor(up, Model.SportType.Bike, Model.IntensityUnit.HeartRate);
        expect_eq_str("155bpm", hr_visitor.getIntensityPretty(intensity_100_pct));
        var intensity_90_pct = new Model.Intensity(0.9, 0.9);
        expect_eq_str("140bpm", hr_visitor.getIntensityPretty(intensity_90_pct));
        var intensity_85_pct = new Model.Intensity(0.85, 0.85);
        expect_eq_str("132bpm", hr_visitor.getIntensityPretty(intensity_85_pct));
    });
});
describe('TSS', function () {
    it('1hr @ 75%', function () {
        var int1hri75 = new Model.ArrayInterval("", [createSimpleInterval(0.75, 3600)]);
        expect_eq_nbr(56.3, int1hri75.getTSS());
    });
    it('1hr @ 85%', function () {
        var int1hri85 = new Model.ArrayInterval("", [createSimpleInterval(0.85, 3600)]);
        expect_eq_nbr(72.2, int1hri85.getTSS());
    });
    it('1min @ 55%-75%', function () {
        var buildInterval = Model.IntervalParser.parse(of_bike, "(1min, 55, 75)");
        expect_eq_nbr(0.7, buildInterval.getTSS());
    });
});
describe('Other Sport Type', function () {
    it('1min @ 55%-75%', function () {
        var buildInterval = Model.IntervalParser.parse(of_other, "(1min, 55, 75)");
        expect_eq_nbr(0.7, buildInterval.getTSS());
    });
});
describe('Bugs', function () {
    it('text causes distance to become miles', function () {
        let d1 = new Model.Duration(Model.DistanceUnit.Yards, 400, 60, 0);
        let d2 = new Model.Duration(Model.TimeUnit.Seconds, 0, 0, 0);
        let cd1 = Model.Duration.combine(d1, d2);
        expect_eq_nbr(Model.DistanceUnit.Yards, cd1.getUnit());
        expect_eq_nbr(400, cd1.getValue());
        let cd2 = Model.Duration.combine(d2, d1);
        expect_eq_nbr(Model.DistanceUnit.Yards, cd2.getUnit());
        expect_eq_nbr(400, cd2.getValue());
    });
});
// TODO: Add test for half MINUTES
describe('Bugs', function () {
    it('Low cadence intervals being parsed as intensity', function () {
        var low_cadence = Model.IntervalParser.parse(of_bike, "(1min, 55, 70rpm)");
        let first_interval = low_cadence.getIntervals()[0];
        expect_eq_nbr(0.55, first_interval.getIntensity().getOriginalValue());
        expect_eq_str("70rpm", first_interval.getTitle());
    });
    it('grade being parsed as intensity', function () {
        var low_cadence = Model.IntervalParser.parse(of_bike, "(1min, 55, 2% grade)");
        let first_interval = low_cadence.getIntervals()[0];
        expect_eq_nbr(0.55, first_interval.getIntensity().getOriginalValue());
        expect_eq_str("2% grade", first_interval.getTitle());
    });
});
describe('Combine duration', function () {
    it('Combine two distances', function () {
        let dur = Model.Duration.combine(new Model.Duration(Model.DistanceUnit.Yards, 100, 0, 0), new Model.Duration(Model.DistanceUnit.Yards, 200, 0, 0));
        expect_eq_nbr(Model.DistanceUnit.Yards, dur.getUnit());
        expect_eq_nbr(300, dur.getValue());
    });
    it('Combine distance and time', function () {
        let interval = Model.IntervalParser.parse(of_swim, `(100yards, 100)(10s, 0)`);
        expect_eq_nbr(100, interval.getTotalDuration().getDistance(Model.DistanceUnit.Yards));
        expect_eq_nbr(95, interval.getTotalDuration().getSeconds(), 1);
    });
    it('Combine durations in a run', function () {
        let interval = Model.IntervalParser.parse(of_run, `(200m, 100)(45s, 100)`);
        expect_eq_nbr(400, interval.getTotalDuration().getDistance(Model.DistanceUnit.Meters), 2);
        expect_eq_nbr(90, interval.getTotalDuration().getSeconds(), 1);
    });
});
describe('IntervalParser', function () {
    it('Parse double', function () {
        expect_eq_nbr(123, Model.IntervalParser.parseDouble("123", 0).value);
        expect_eq_nbr(123.456, Model.IntervalParser.parseDouble("123.456", 0).value);
    });
    it('1hr @ 75%', function () {
        var int_par_1hr_75 = Model.IntervalParser.parse(of_bike, "(1hr, 75)");
        expect_eq_nbr(56.3, int_par_1hr_75.getTSS());
    });
    it('1hr @ 75%, 1hr @ 85%', function () {
        var int_par_2hr_75_85 = Model.IntervalParser.parse(of_bike, "(1hr, 75), (1hr, 85)");
        expect_eq_nbr(128.5, int_par_2hr_75_85.getTSS());
    });
    it('1min @ 85%', function () {
        var simple1min85 = Model.IntervalParser.parse(of_bike, "(1, 85)");
        expect_eq_nbr(0.85, simple1min85.getIntervals()[0].getIntensity().getOriginalValue());
    });
    it('1min @ 55%-85%', function () {
        var build1min5585 = Model.IntervalParser.parse(of_bike, "(1, 60, 80)");
        expect_eq_nbr(0.70, build1min5585.getIntervals()[0].getIntensity().getOriginalValue());
    });
    it('Mixed time and distance', function () {
        var int_par_bug = Model.IntervalParser.parse(of_bike, "(1hr, 75), (20mi, 85)");
        expect_eq_nbr(0.75, int_par_bug.getIntervals()[0].getIntensity().getOriginalValue());
        expect_eq_nbr(0.85, int_par_bug.getIntervals()[1].getIntensity().getOriginalValue());
    });
    it('Mixed watts and percentage', function () {
        var int_par_unit_bike = Model.IntervalParser.parse(of_bike, "(1s, 50), (2s, 155w), (3s, 50%)");
        expect_eq_nbr(Model.IntensityUnit.IF, int_par_unit_bike.getIntervals()[0].getIntensity().getOriginalUnit());
        expect_eq_nbr(0.5, int_par_unit_bike.getIntervals()[0].getIntensity().getOriginalValue());
        expect_eq_nbr(Model.IntensityUnit.Watts, int_par_unit_bike.getIntervals()[1].getIntensity().getOriginalUnit());
        expect_eq_nbr(155, int_par_unit_bike.getIntervals()[1].getIntensity().getOriginalValue());
        expect_eq_nbr(Model.IntensityUnit.IF, int_par_unit_bike.getIntervals()[2].getIntensity().getOriginalUnit());
        expect_eq_nbr(0.5, int_par_unit_bike.getIntervals()[2].getIntensity().getOriginalValue());
    });
    it('Mixed mph and min/mi', function () {
        var int_par_unit_run = Model.IntervalParser.parse(of_run, "(4s, 10mph), (5s, 6min/mi)");
        expect_eq_nbr(Model.IntensityUnit.Mph, int_par_unit_run.getIntervals()[0].getIntensity().getOriginalUnit());
        expect_eq_nbr(10, int_par_unit_run.getIntervals()[0].getIntensity().getOriginalValue());
        expect_eq_nbr(Model.IntensityUnit.MinMi, int_par_unit_run.getIntervals()[1].getIntensity().getOriginalUnit());
        expect_eq_nbr(6, int_par_unit_run.getIntervals()[1].getIntensity().getOriginalValue());
    });
    it('no intensity', function () {
        expect_eq_nbr(0, Model.IntervalParser.parse(of_bike, `(10min)`).getIntensity().getValue());
        expect_eq_nbr(0, Model.IntervalParser.parse(of_bike, `(10min, easy)`).getIntensity().getValue());
        expect_eq_nbr(0.55, Model.IntervalParser.parse(of_bike, `(10s, *, free ride)`).getIntensity().getValue());
    });
    it('repeat interval', function () {
        var repeat_85_1 = Model.IntervalParser.parse(of_bike, `1[(1hr, 85)]`);
        var repeat_85_2 = Model.IntervalParser.parse(of_bike, `2[(1hr, 85)]`);
        expect_true(repeat_85_1.getIntervals()[0] instanceof Model.RepeatInterval);
        expect_eq_nbr(72.2, repeat_85_1.getTSS());
        expect_eq_nbr(3600, repeat_85_1.getWorkDuration().getSeconds());
        expect_eq_nbr(0.85, repeat_85_1.getIntensity().getValue());
        expect_eq_nbr(144.5, repeat_85_2.getTSS());
        expect_eq_nbr(2 * 3600, repeat_85_2.getWorkDuration().getSeconds());
        expect_eq_nbr(0.85, repeat_85_2.getIntensity().getValue());
    });
    it('baseline plus repeat', function () {
        var baseLinePlusRepeat = Model.IntervalParser.parse(of_bike, `(1hr, 75) 2[(1hr, 85)]`);
        expect_eq_nbr(200.8, baseLinePlusRepeat.getTSS(), 0.1);
        expect_eq_nbr(3 * 3600, baseLinePlusRepeat.getWorkDuration().getSeconds());
        expect_eq_nbr(0.816, baseLinePlusRepeat.getIntensity().getValue());
    });
    it('one hour 85', function () {
        var onehr85 = Model.IntervalParser.parse(of_bike, `(1hr, 86)`);
        expect_eq_nbr(74, onehr85.getTSS(), 0.1);
        expect_eq_nbr(0.86, onehr85.getIntensity().getValue());
        expect_eq_nbr(3600, onehr85.getWorkDuration().getSeconds());
    });
    it('one hour high tss', function () {
        var onehr85ButHighTSS = Model.IntervalParser.parse(of_bike, `(30min, 60), (30min, 100)`);
        expect_eq_nbr(68, onehr85ButHighTSS.getTSS(), 0.1);
        expect_eq_nbr(0.82, onehr85ButHighTSS.getIntensity().getValue());
        expect_eq_nbr(3600, onehr85ButHighTSS.getWorkDuration().getSeconds());
    });
    it('step build', function () {
        var step_build_85_100 = Model.IntervalParser.parse(of_bike, `3[(1min, 80, 90, 100), (30s, 50)]`);
        expect_eq_nbr(4.7, step_build_85_100.getTSS(), 0.1);
        expect_eq_nbr(3 * 60 + 3 * 30, step_build_85_100.getWorkDuration().getSeconds());
        expect_eq_nbr(0.85, step_build_85_100.getIntensity().getValue());
        expect_true(!step_build_85_100.getIntervals()[0].areAllIntensitiesSame());
    });
    it('step build equal intensity', function () {
        var step_build_equal_intensity = Model.IntervalParser.parse(of_bike, `3[(2min, 1min, 1min, 75), (1min, 55)]`);
        expect_eq_nbr((2 + 1 + 1) * 60 + 3 * 60, step_build_equal_intensity.getWorkDuration().getSeconds());
        expect_true(step_build_equal_intensity.getIntervals()[0].areAllIntensitiesSame());
    });
    it('repeat interval', function () {
        var repeat_main_interval = Model.IntervalParser.parse(of_bike, `4[(45s, 75, 100), (15s, 55)]`);
        expect_eq_nbr(1, repeat_main_interval.getIntervals().length);
        var repeat_interval = repeat_main_interval.getIntervals()[0];
        expect_eq_nbr(4, repeat_interval.getRepeatCount());
    });
    it('min/mi', function () {
        var units_on_workout = Model.IntervalParser.parse(of_run, `(60min, 6:00min/mi)`);
        expect_eq_nbr(3600, units_on_workout.getWorkDuration().getSeconds());
        expect_eq_nbr(1, units_on_workout.getIntensity().getValue());
        var units_on_workout_spc = Model.IntervalParser.parse(of_run, `(60min, 6:00 min/mi)`);
        expect_eq_nbr(3600, units_on_workout_spc.getWorkDuration().getSeconds());
        expect_eq_nbr(1, units_on_workout_spc.getIntensity().getValue());
    });
    it('min/km', function () {
        var units_on_workout_2 = Model.IntervalParser.parse(of_run, `(45min, 3:44min/km)`);
        expect_eq_nbr(45 * 60, units_on_workout_2.getWorkDuration().getSeconds());
        expect_eq_nbr(1, units_on_workout_2.getIntensity().getValue());
    });
    it('swim duration and tss', function () {
        expect_eq_nbr(425, Model.IntervalParser.parse(of_swim, `(500yards, 100, warmup)`).getWorkDuration().getSeconds());
        expect_eq_nbr(425, Model.IntervalParser.parse(of_swim, `(500yards, warmup, 100)`).getWorkDuration().getSeconds());
        expect_eq_nbr(11.8, Model.IntervalParser.parse(of_swim, `(500yards, 100, warmup)`).getTSS());
        expect_eq_nbr(15.6, Model.IntervalParser.parse(of_swim, `(500yards, 100, warmup), (200yards, 80, easy)`).getTSS());
    });
    it("swim duration and tss (meters)", function () {
        expect_eq_nbr(464.78, Model.IntervalParser.parse(of_swim, `(500m, 100, warmup)`).getWorkDuration().getSeconds());
        expect_eq_nbr(464.78, Model.IntervalParser.parse(of_swim, `(500m, warmup, 100)`).getWorkDuration().getSeconds());
        expect_eq_nbr(12.9, Model.IntervalParser.parse(of_swim, `(500m, 100, warmup)`).getTSS());
        expect_eq_nbr(17, Model.IntervalParser.parse(of_swim, `(500m, 100, warmup), (200m, 80, easy)`).getTSS());
    });
});
describe('UserProfile', function () {
    it('6min/mi to other paces', function () {
        var up_6tpace = new Model.UserProfile(310, "6:00min/mi");
        expect_eq_nbr(6, up_6tpace.getPaceMinMi(new Model.Intensity(1, 1)));
        expect_eq_nbr(7.05, up_6tpace.getPaceMinMi(new Model.Intensity(0.85, 0.85)));
        expect_eq_nbr(8, up_6tpace.getPaceMinMi(new Model.Intensity(0.75, 0.75)));
    });
});
describe('File Generation', function () {
    var file_interval_simple = Model.IntervalParser.parse(of_bike, "(10min, 55, 75), (1hr, 80), (5min, 75, 55)");
    var file_interval_free_ride = Model.IntervalParser.parse(of_bike, "(10min, 55, 75), (1hr, *, Free), (5min, 75, 55)");
    it('Zwift Simple', function () {
        var zwift = new Model.ZwiftDataVisitor("untitled_workout");
        Model.VisitorHelper.visitAndFinalize(zwift, file_interval_simple);
        var expected_content = `<workout_file>
	<author>Workout Planner Author</author>
	<name>untitled_workout</name>
	<description>Auto generated by https://github.com/sergioclemente/WorkoutPlanner</description>
	<tags>
		<tag name="INTERVALS"/>
	</tags>
	<workout>
		<Warmup Duration="600" PowerLow="0.55" PowerHigh="0.75"/>
		<SteadyState Duration="3600" Power="0.8">
			<textevent timeoffset="0" message="1hr @ 80%"/>
		</SteadyState>
		<Cooldown Duration="300" PowerLow="0.75" PowerHigh="0.55"/>
	</workout>
</workout_file>`;
        expect_eq_str(expected_content, zwift.getContent());
    });
    it('Zwift Free Ride', function () {
        var zwift = new Model.ZwiftDataVisitor("untitled_workout");
        Model.VisitorHelper.visitAndFinalize(zwift, file_interval_free_ride);
        var expected_content = `<workout_file>
	<author>Workout Planner Author</author>
	<name>untitled_workout</name>
	<description>Auto generated by https://github.com/sergioclemente/WorkoutPlanner</description>
	<tags>
		<tag name="INTERVALS"/>
	</tags>
	<workout>
		<Warmup Duration="600" PowerLow="0.55" PowerHigh="0.75"/>
		<FreeRide Duration="3600">
			<textevent timeoffset="0" message="Free"/>
		</FreeRide>
		<Cooldown Duration="300" PowerLow="0.75" PowerHigh="0.55"/>
	</workout>
</workout_file>`;
        expect_eq_str(expected_content, zwift.getContent());
    });
    it('MRC Simple', function () {
        var mrc = new Model.MRCCourseDataVisitor("untitled_workout");
        Model.VisitorHelper.visitAndFinalize(mrc, file_interval_simple);
        var expected_content = `[COURSE HEADER]
VERSION=2
UNITS=ENGLISH
FILE NAME=untitled_workout
MINUTES\tPERCENT
[END COURSE HEADER]
[COURSE DATA]
0\t55
10\t75
10\t80
70\t80
70\t75
75\t55
[END COURSE DATA]
[PERFPRO DESCRIPTIONS]
Desc0=10' warm up to 75%
Desc1=1hr @ 80%
Desc2=5' warm down from 75% to 55%
[END PERFPRO DESCRIPTIONS]
`;
        expect_eq_str(expected_content, mrc.getContent());
    });
    it('MRC more complex', function () {
        var file_interval = Model.IntervalParser.parse(of_bike, "(10min, 55, 75), 4[(1hr, 80), (5min, 55)], (20min, 75)");
        var mrc = new Model.MRCCourseDataVisitor("untitled_workout");
        Model.VisitorHelper.visitAndFinalize(mrc, file_interval);
        var expected_content = `[COURSE HEADER]
VERSION=2
UNITS=ENGLISH
FILE NAME=untitled_workout
MINUTES\tPERCENT
[END COURSE HEADER]
[COURSE DATA]
0\t55
10\t75
10\t80
70\t80
70\t55
75\t55
75\t80
135\t80
135\t55
140\t55
140\t80
200\t80
200\t55
205\t55
205\t80
265\t80
265\t55
270\t55
270\t75
290\t75
[END COURSE DATA]
[PERFPRO DESCRIPTIONS]
Desc0=10' warm up to 75%
Desc1=1hr @ 80% | 1 of 4
Desc2=5' easy | 1 of 4
Desc3=1hr @ 80% | 2 of 4
Desc4=5' easy | 2 of 4
Desc5=1hr @ 80% | 3 of 4
Desc6=5' easy | 3 of 4
Desc7=1hr @ 80% | 4 of 4
Desc8=5' easy | 4 of 4
Desc9=20' @ 75%
[END PERFPRO DESCRIPTIONS]
`;
        expect_eq_str(expected_content, mrc.getContent());
    });
    it('PPSMRX Free Ride', function () {
        var zwift = new Model.PPSMRXCourseDataVisitor("untitled_workout");
        Model.VisitorHelper.visitAndFinalize(zwift, file_interval_free_ride);
        var expected_content = `{
	"type":"json",
	"createdby":"PerfPRO Studio v5.80.25",
	"version":5.00,
	"name":"manual mode",
	"workoutType":"",
	"comments":"",
	"isLocked":0,
	"videoFile":"",
	"showCountDown":0,
	"showStep":0,
	"movieMode":0,
	"startMinute":0,
	"set_fields":["description","seconds","start","finish","mode","intervals","group","autolap","targetcad"],
	"sets":[
		["10' warm up to 75%",600,55,75,"M",1,75,0,90],
		["Free",3600,0,0,"T",1,0,0,90],
		["5' warm down from 75% to 55%",300,75,55,"M",1,55,0,90]
	]
}`;
        expect_eq_str(expected_content, zwift.getContent());
    });
    it('PPSMRX More Complex', function () {
        let file_interval = Model.IntervalParser.parse(of_bike, "(10min, 55, 75), 4[(1hr, 80), (5min, 55)], (20min, 75)");
        let mrx_visitor = new Model.PPSMRXCourseDataVisitor("untitled_workout");
        Model.VisitorHelper.visitAndFinalize(mrx_visitor, file_interval);
        let expected_content = `{
	"type":"json",
	"createdby":"PerfPRO Studio v5.80.25",
	"version":5.00,
	"name":"manual mode",
	"workoutType":"",
	"comments":"",
	"isLocked":0,
	"videoFile":"",
	"showCountDown":0,
	"showStep":0,
	"movieMode":0,
	"startMinute":0,
	"set_fields":["description","seconds","start","finish","mode","intervals","group","autolap","targetcad"],
	"sets":[
		["10' warm up to 75%",600,55,75,"M",1,75,0,90],
		["1hr @ 80% (1/4)",3600,80,80,"M",1,1,0,90],
		["5' easy (1/4)",300,55,55,"M",1,1,0,90],
		["1hr @ 80% (2/4)",3600,80,80,"M",1,1,0,90],
		["5' easy (2/4)",300,55,55,"M",1,1,0,90],
		["1hr @ 80% (3/4)",3600,80,80,"M",1,1,0,90],
		["5' easy (3/4)",300,55,55,"M",1,1,0,90],
		["1hr @ 80% (4/4)",3600,80,80,"M",1,1,0,90],
		["5' easy (4/4)",300,55,55,"M",1,1,0,90],
		["20' @ 75%",1200,75,75,"M",1,0,0,90]
	]
}`;
        expect_eq_str(expected_content, mrx_visitor.getContent());
    });
});
// StepBuildInterval basic tests
var duration1min = new Model.Duration(Model.TimeUnit.Seconds, 60, 60, 0);
var duration30s = new Model.Duration(Model.TimeUnit.Seconds, 30, 30, 0);
var si1min80 = new Model.SimpleInterval("", new Model.Intensity(80), duration1min);
var si1min90 = new Model.SimpleInterval("", new Model.Intensity(90), duration1min);
var si1min100 = new Model.SimpleInterval("", new Model.Intensity(100), duration1min);
var si30s50 = new Model.SimpleInterval("", new Model.Intensity(50), duration30s);
var sbi = new Model.StepBuildInterval("", [si1min80, si1min90, si1min100, si30s50]);
expect_eq_nbr(3, sbi.getRepeatCount());
expect_eq_nbr(3 * 60 + 3 * 30, sbi.getWorkDuration().getSeconds());
describe('zone visitor', function () {
    it('1', function () {
        expect_eq_nbr(1, Model.ZonesVisitor.getZone(Model.SportType.Bike, 0.50));
    });
    it('2', function () {
        expect_eq_nbr(2, Model.ZonesVisitor.getZone(Model.SportType.Bike, 0.55));
    });
    it('3', function () {
        expect_eq_nbr(3, Model.ZonesVisitor.getZone(Model.SportType.Bike, 0.75));
    });
    it('4', function () {
        expect_eq_nbr(4, Model.ZonesVisitor.getZone(Model.SportType.Bike, 0.90));
    });
    it('5', function () {
        expect_eq_nbr(5, Model.ZonesVisitor.getZone(Model.SportType.Bike, 1.05));
    });
});
describe('UI field validator', function () {
    it('email', function () {
        expect_true(!UI.FieldValidator.validateEmail(""));
        expect_true(!UI.FieldValidator.validateEmail("invalid"));
        expect_true(!UI.FieldValidator.validateEmail("@bar.com"));
        expect_true(UI.FieldValidator.validateEmail("foo@bar.com"));
    });
    it('number', function () {
        expect_true(!UI.FieldValidator.validateNumber(""));
        expect_true(!UI.FieldValidator.validateNumber("asd123asd"));
        expect_true(UI.FieldValidator.validateNumber("123"));
    });
    it('speed mph', function () {
        expect_eq_nbr(8, Model.SpeedParser.getSpeedInMph("7:30min/mi"));
        expect_eq_nbr(8, Model.SpeedParser.getSpeedInMph("8mi/h"));
        expect_eq_nbr(8.44, Model.SpeedParser.getSpeedInMph("4:25min/km"));
        expect_eq_nbr(9.94, Model.SpeedParser.getSpeedInMph("1:30/400meters"));
    });
});
describe('Interval title', function () {
    it('simple interval', function () {
        expect_eq_str("10' easy", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(10min, 55)`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("10'' relaxed @ 10:00min/mi", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(10s, 60, relaxed)`), up, Model.SportType.Run, Model.IntensityUnit.MinMi));
        expect_eq_str("10'' relaxed", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(10s, relaxed)`), up, Model.SportType.Run, Model.IntensityUnit.MinMi));
        expect_eq_str("10' single leg @ 140w", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(10min, 45, single leg)`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("10' @ 205w", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(10min, 65)`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("10' LC @ 235w", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(10min, 75, LC)`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("10' @ 65%", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(10min, 65)`), up, Model.SportType.Bike, Model.IntensityUnit.IF));
        expect_eq_str("1mi threshold @ 1:29/400meters", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(1mi, 100, threshold)`), up, Model.SportType.Run, Model.IntensityUnit.Per400Meters));
    });
    it('warmup/build/warm down', function () {
        expect_eq_str("10' warm up to 235w", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(55, 75, 10min)`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("10' build from 235w to 310w", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(75, 100, 10min)`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("10' warm down from 310w to 235w", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(100, 75, 10min)`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
    });
    it('free ride', function () {
        expect_eq_str("10' TT", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(10min, *, TT)`), up, Model.SportType.Swim, Model.IntensityUnit.Per100Yards));
        expect_eq_str("10' TT", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_bike, `(10min, *, TT)`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("10' TT", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(10min, *, TT)`), up, Model.SportType.Run, Model.IntensityUnit.MinMi));
    });
    it('repeat w/ 2 watts', function () {
        expect_eq_str("2 x (5' @ 235w - 2' @ 310w)", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `2[(5min, 75), (2min, 100)]`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("2 x 5' @ 235w - 1' easy", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `2[(5min, 75), (1min, 55)]`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("2 x (5' @ 235w, 2' @ 310w) - w/ 1' easy", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `2[(5min, 75), (2min, 100), (1min, 55)]`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("2 x (5' @ 235w, 2' @ 310w) - w/ 1' @ 205w", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `2[(5min, 75), (2min, 100), (1min, 65)]`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("2 x 5' @ 190w - 1' easy", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `2[(5min, 60), (1min, 55)]`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
    });
    it('repeat w/ 2 times', function () {
        expect_eq_str("2 x 2' - w/ 1' easy (235w, 265w)", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `2[(75, 85, 2min), (1min, 55)]`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
        expect_eq_str("2 x 235w - w/ 1' easy (1', 3')", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `2[(75, 1min, 3min), (1min, 55)]`), up, Model.SportType.Bike, Model.IntensityUnit.Watts));
    });
    it('comment', function () {
        expect_eq_str("Do this in a flat terrain", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `"Do this in a flat terrain"`), up, Model.SportType.Bike, Model.IntensityUnit.IF));
        expect_eq_str("Do this in a flat terrain - 10' easy", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `"Do this in a flat terrain", (10min, 55)`), up, Model.SportType.Bike, Model.IntensityUnit.IF));
        expect_eq_str("Do this in a flat terrain - 10' easy", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `"Do this in a flat terrain",(10min, 55)`), up, Model.SportType.Bike, Model.IntensityUnit.IF));
        expect_eq_str("10' easy - Do this in a flat terrain", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_run, `(10min, 55), "Do this in a flat terrain"`), up, Model.SportType.Bike, Model.IntensityUnit.IF));
    });
    it('swim', function () {
        expect_eq_str("500 warmup on 7'52'' off 8'42''", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(500yards, 90, 50s, warmup)`), up, Model.SportType.Swim, Model.IntensityUnit.Per100Yards));
        expect_eq_str("100 strong on 1'25'' off 1'35''", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(100yards, 100, 10s, strong)`), up, Model.SportType.Swim, Model.IntensityUnit.Per100Yards));
        expect_eq_str("500 warmup on 7'55'' off 8'45''", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(500yards, +10, 50s, warmup)`), up, Model.SportType.Swim, Model.IntensityUnit.Per100Yards));
        expect_eq_str("500 warmup w/ 50'' rest", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(500yards, 50s, warmup)`), up, Model.SportType.Swim, Model.IntensityUnit.Per100Yards));
        expect_eq_str("500 warmup on 6'40'' off 7'30''", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(500yards, -5, 50s, warmup)`), up, Model.SportType.Swim, Model.IntensityUnit.Per100Yards));
        expect_eq_str("500 warmup on 6'14'' off 7'04''", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(500yards, -10, 50s, warmup)`), up, Model.SportType.Swim, Model.IntensityUnit.Per100Yards));
        expect_eq_str("500 warmup w/ 50'' rest", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(500yards, 50s, warmup)`), up, Model.SportType.Swim, Model.IntensityUnit.IF));
        expect_eq_str("500 warmup on 7'55'' off 8'45''", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(500yards, +10, 50s, warmup)`), up, Model.SportType.Swim, Model.IntensityUnit.IF));
        expect_eq_str("100 strong on 1'20'' off 1'35''", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(100yards, -5, 15s, strong)`), up, Model.SportType.Swim, Model.IntensityUnit.IF));
        expect_eq_str("100 strong on 1'25'' off 1'45''", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(100yards, +0, 20s, strong)`), up, Model.SportType.Swim, Model.IntensityUnit.IF));
        expect_eq_str("100 strong on 1'25'' off 1'30''", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(100yards, -0, 5s, strong)`), up, Model.SportType.Swim, Model.IntensityUnit.IF));
        expect_eq_str("400 free", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(400yards, free)`), up, Model.SportType.Swim, Model.IntensityUnit.IF));
        expect_eq_str("300 pull", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(300yards, pull)`), up, Model.SportType.Swim, Model.IntensityUnit.IF));
        expect_eq_str("300 pull - 10'' rest", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(300yards, pull), (10s, rest)`), up, Model.SportType.Swim, Model.IntensityUnit.IF));
        expect_eq_str("300 pull w/ 10'' rest", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(300yards, pull, 10s)`), up, Model.SportType.Swim, Model.IntensityUnit.IF));
    });
});
describe('Formatter', function () {
    it('number formatter', function () {
        expect_eq_str("8:00min/mi", Model.FormatterHelper.formatNumber(8, 60, ":", "min/mi", 0));
        expect_eq_str("8:00min/mi", Model.FormatterHelper.formatNumber(8, 60, ":", "min/mi", 5));
        expect_eq_str("7:30min/mi", Model.FormatterHelper.formatNumber(7.5, 60, ":", "min/mi", 0));
        expect_eq_str("7:30min/mi", Model.FormatterHelper.formatNumber(7.5, 60, ":", "min/mi", 5));
        expect_eq_str("7:08min/mi", Model.FormatterHelper.formatNumber(7.13, 60, ":", "min/mi", 0));
        expect_eq_str("7:05min/mi", Model.FormatterHelper.formatNumber(7.13, 60, ":", "min/mi", 5));
        expect_eq_str("7:46min/mi", Model.FormatterHelper.formatNumber(7.77, 60, ":", "min/mi", 0));
        expect_eq_str("7:45min/mi", Model.FormatterHelper.formatNumber(7.77, 60, ":", "min/mi", 5));
        expect_eq_str("7:14min/mi", Model.FormatterHelper.formatNumber(7.23, 60, ":", "min/mi", 0));
        expect_eq_str("7:10min/mi", Model.FormatterHelper.formatNumber(7.23, 60, ":", "min/mi", 5));
    });
    it('number rounder (up)', function () {
        expect_eq_nbr(240, Model.FormatterHelper.roundNumberUp(240, 5));
        expect_eq_nbr(245, Model.FormatterHelper.roundNumberUp(241, 5));
        expect_eq_nbr(245, Model.FormatterHelper.roundNumberUp(242, 5));
        expect_eq_nbr(245, Model.FormatterHelper.roundNumberUp(243, 5));
        expect_eq_nbr(245, Model.FormatterHelper.roundNumberUp(244, 5));
        expect_eq_nbr(245, Model.FormatterHelper.roundNumberUp(245, 5));
        expect_eq_nbr(250, Model.FormatterHelper.roundNumberUp(246, 5));
    });
    it('number rounder (down)', function () {
        expect_eq_nbr(240, Model.FormatterHelper.roundNumberDown(240, 5));
        expect_eq_nbr(240, Model.FormatterHelper.roundNumberDown(241, 5));
        expect_eq_nbr(240, Model.FormatterHelper.roundNumberDown(242, 5));
        expect_eq_nbr(240, Model.FormatterHelper.roundNumberDown(243, 5));
        expect_eq_nbr(240, Model.FormatterHelper.roundNumberDown(244, 5));
        expect_eq_nbr(245, Model.FormatterHelper.roundNumberDown(245, 5));
        expect_eq_nbr(245, Model.FormatterHelper.roundNumberDown(246, 5));
    });
});
describe('Swim', function () {
    it('speed conversion', function () {
        expect_eq_nbr(2.41, Model.IntensityUnitHelper.convertTo(1.41, Model.IntensityUnit.Per100Yards, Model.IntensityUnit.Mph));
        expect_eq_nbr(1.42, Model.IntensityUnitHelper.convertTo(2.4, Model.IntensityUnit.Mph, Model.IntensityUnit.Per100Yards));
        expect_eq_nbr(2.40, Model.SpeedParser.getSpeedInMph("1:25/100yards"));
        expect_eq_nbr(2.63, Model.SpeedParser.getSpeedInMph("1:25/100meters"));
    });
    it('intensity conversion', function () {
        expect_eq_str("1:30/100yards", new Model.Intensity(1, 1.5, Model.IntensityUnit.Per100Yards).toString());
        var swim_visitor_mph = new Model.WorkoutTextVisitor(up, Model.SportType.Swim, Model.IntensityUnit.Mph);
        var swim_visitor_per100 = new Model.WorkoutTextVisitor(up, Model.SportType.Swim, Model.IntensityUnit.Per100Yards);
        expect_eq_str("2.4mi/h", swim_visitor_mph.getIntensityPretty(intensity_100_pct));
        expect_eq_str("1:25/100yards", swim_visitor_per100.getIntensityPretty(intensity_100_pct));
    });
    it('speed conversion (meters)', function () {
        expect_eq_str("500 warmup on 8'36''", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(500m, 90, warmup)`), up, Model.SportType.Swim, Model.IntensityUnit.Per100Meters));
        expect_eq_str("100 strong on 1'32''", Model.WorkoutTextVisitor.getIntervalTitle(Model.IntervalParser.parse(of_swim, `(100m, 100, strong)`), up, Model.SportType.Swim, Model.IntensityUnit.Per100Meters));
        expect_eq_nbr(2.48, Model.IntensityUnitHelper.convertTo(1.5, Model.IntensityUnit.Per100Meters, Model.IntensityUnit.Mph));
        expect_eq_nbr(1.5, Model.IntensityUnitHelper.convertTo(2.48, Model.IntensityUnit.Mph, Model.IntensityUnit.Per100Meters));
        expect_eq_str("1:30/100meters", new Model.Intensity(1, 1.5, Model.IntensityUnit.Per100Meters).toString());
        var swim_visitor_mph = new Model.WorkoutTextVisitor(up, Model.SportType.Swim, Model.IntensityUnit.Mph);
        var swim_visitor_per100 = new Model.WorkoutTextVisitor(up, Model.SportType.Swim, Model.IntensityUnit.Per100Meters);
        expect_eq_str("2.4mi/h", swim_visitor_mph.getIntensityPretty(intensity_100_pct));
        expect_eq_str("1:33/100meters", swim_visitor_per100.getIntensityPretty(intensity_100_pct));
    });
    it('speed', function () {
        expect_eq_nbr(/* 1:25 */ 60 + 25, new Model.ObjectFactory(up, Model.SportType.Swim).createDuration(intensity_100_pct, Model.DistanceUnit.Yards, 100).getSeconds());
    });
    it('speed (meters)', function () {
        expect_eq_nbr(/* 1:32 */ 92.95, new Model.ObjectFactory(up, Model.SportType.Swim).createDuration(intensity_100_pct, Model.DistanceUnit.Meters, 100).getSeconds());
    });
    it('distance and rest interval', function () {
        let interval = Model.IntervalParser.parse(of_swim, `2[(200yards, neg split, 30s)]`);
        expect_eq_nbr(400, interval.getTotalDuration().getDistance(Model.DistanceUnit.Yards));
        expect_eq_nbr(30, interval.getRestDuration().getSeconds());
    });
});
describe('Player Helper', function () {
    it('Two intervals, 2 boundaries', function () {
        let interval = Model.IntervalParser.parse(of_swim, "(10min, 55, t1), (20min, 60, t2)");
        let ph = new Model.PlayerHelper(interval);
        expect_eq_str("t1", ph.get(0).getInterval().getTitle());
        expect_eq_str("t1", ph.get(10 * 60).getInterval().getTitle());
        expect_eq_str("t2", ph.get(20 * 60).getInterval().getTitle());
        expect_eq_str("t2", ph.get(30 * 60).getInterval().getTitle());
        expect_eq_str(null, ph.get(40 * 60));
    });
});
describe('free ride', function () {
    it('simple free ride', function () {
        let array_interval = Model.IntervalParser.parse(of_swim, "(10min, *, TT)");
        let free_ride_interval = array_interval.getIntervals()[0];
        expect_eq_nbr(Model.IntensityUnit.FreeRide, free_ride_interval.getIntensity().getOriginalUnit());
        expect_eq_str("TT", free_ride_interval.getTitle());
    });
});
describe('rest interval ride', function () {
    it('simple rest interval', function () {
        let array_interval = Model.IntervalParser.parse(of_swim, "(10min, 100, 2min, FTP)");
        let free_ride_interval = array_interval.getIntervals()[0];
        expect_eq_nbr(1, free_ride_interval.getIntensity().getValue());
        expect_eq_nbr(10 * 60 + 2 * 60, free_ride_interval.getTotalDuration().getSeconds());
        expect_eq_str("FTP", free_ride_interval.getTitle());
    });
});
describe('NumberAndUnitParser', function () {
    it('Per400m', function () {
        let p = new Model.NumberAndUnitParser();
        p.evaluate("1:30/400m", 0);
        expect_eq_nbr(1.5, p.getValue());
        expect_eq_str("/400m", p.getUnit());
    });
});
function parseAndNormalize(of, text, expected_text = null) {
    let normalized_text = Model.IntervalParser.normalize(of, text);
    if (expected_text == null) {
        expected_text = text;
    }
    expect_eq_str(expected_text, normalized_text);
}
describe('parse and unparse', function () {
    it('simple', function () {
        parseAndNormalize(of_bike, "(10min, 100, FTP)");
        parseAndNormalize(of_run, "(2mi, 6:00min/mi, threshold)", "(2mi, 100, threshold)");
        parseAndNormalize(of_swim, "\"Comment 123\"");
        parseAndNormalize(of_bike, "(155w, 310w, 1min, build to ftp)", "(50, 100, 1min, build to ftp)");
        parseAndNormalize(of_bike, "4[(1min, 100, hard), (30sec, 50, easy)]");
        parseAndNormalize(of_swim, "(400yards, +10s, 30sec, warmup)");
        parseAndNormalize(of_run, "(2mi, 80)");
        parseAndNormalize(of_bike, "2[(1min, 85, 95), (30sec, 50)]");
        parseAndNormalize(of_bike, "2[(100, 30sec, 45sec), (30sec, 50)]");
    });
});
describe("Tests Log has basic tags", function(){
    it("has INFO tag", function(){
        expect(typeof Log.i).toBeDefined();
        expect(typeof Log.i).toEqual("function");
    });

    it("has DEBUG tag", function(){
        expect(typeof Log.d).toBeDefined();
        expect(typeof Log.d).toEqual("function");
    });

    it("has ERROR tag", function(){
        expect(typeof Log.e).toBeDefined();
        expect(typeof Log.e).toEqual("function");
    });
});

describe("Tests Log.write()", function(){
    var goodData: string;
    var goodTag: string;
    var goodLocation: string;

    beforeEach(function(){
        goodData = "test log";
        goodTag = Log.DEBUG_TAG;
        goodLocation = "LogHelperTest S2";

        spyOn(console, "log");
    });

    it("has good data, good tag, good location", function(){
        Log.write(goodData, goodTag, goodLocation);

        expect(console.log).toHaveBeenCalledWith("[" + goodTag + "] ", goodLocation + " - ", goodData);
    });

    it("has good data, good tag, bad location", function(){
        Log.write(goodData, goodTag, undefined);

        expect(console.log).toHaveBeenCalledWith("[" + goodTag + "] - ", "", goodData);
    });

    it("has has good data, bad tag, good location", function(){
        Log.write(goodData, undefined, goodLocation);

        expect(console.log).toHaveBeenCalledWith("", goodLocation + " - ", goodData);
    });

    it("has bad data, good tag, good location", function(){
        Log.write(undefined, goodTag, goodLocation);

        expect(console.log).toHaveBeenCalledWith("[" + goodTag + "] ", goodLocation + " - ", undefined);
    });

    it("has good data, bad tag, bad location", function(){
        Log.write(goodData, undefined, undefined);

        expect(console.log).toHaveBeenCalledWith("", "", goodData);
    });

    it("has bad data, good tag, bad location", function(){
        Log.write(undefined, goodTag, undefined);

        expect(console.log).toHaveBeenCalledWith("[" + goodTag + "] - ", "", undefined);
    });

    it("has bad data, bad tag, good location", function(){
        Log.write(undefined, undefined, goodLocation);

        expect(console.log).toHaveBeenCalledWith("", goodLocation + " - ", undefined);
    });

    it("has bad data, bad tag, bad location", function(){
        Log.write(undefined, undefined, undefined);

        expect(console.log).toHaveBeenCalledWith("", "", undefined);
    });
});

describe("Tests Log.mute()", function(){
    var logData;
    var logLocation;

    var iLogArgs;
    var dLogArgs;
    var eLogArgs;

    beforeEach(function(){
        logData = "test log";
        logLocation = "LogHelperTest S3";

        iLogArgs = ["[" + Log.INFO_TAG + "] ", logLocation + " - ", logData];
        dLogArgs = ["[" + Log.DEBUG_TAG + "] ", logLocation + " - ", logData];
        eLogArgs = ["[" + Log.ERROR_TAG + "] ", logLocation + " - ", logData];

        spyOn(console, "log");
    });

    afterEach(function(){
        Log.unmute();
    });

    function callAllTags(){
        Log.i(logData, logLocation);
        Log.d(logData, logLocation);
        Log.e(logData, logLocation);
    }

    it("mutes INFO", function(){
        Log.mute(Log.INFO_TAG);
        callAllTags();

        expect(console.log).not.toHaveBeenCalledWith(iLogArgs[0], iLogArgs[1], iLogArgs[2]);
        expect(console.log).toHaveBeenCalledWith(dLogArgs[0], dLogArgs[1], dLogArgs[2]);
        expect(console.log).toHaveBeenCalledWith(eLogArgs[0], eLogArgs[1], eLogArgs[2]);
    });

    it("mutes DEBUG", function(){
        Log.mute(Log.DEBUG_TAG);
        callAllTags();

        expect(console.log).toHaveBeenCalledWith(iLogArgs[0], iLogArgs[1], iLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(dLogArgs[0], dLogArgs[1], dLogArgs[2]);
        expect(console.log).toHaveBeenCalledWith(eLogArgs[0], eLogArgs[1], eLogArgs[2]);
    });

    it("mutes ERROR", function(){
        Log.mute(Log.ERROR_TAG);
        callAllTags();

        expect(console.log).toHaveBeenCalledWith(iLogArgs[0], iLogArgs[1], iLogArgs[2]);
        expect(console.log).toHaveBeenCalledWith(dLogArgs[0], dLogArgs[1], dLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(eLogArgs[0], eLogArgs[1], eLogArgs[2]);
    });

    it("mutes all", function(){
        Log.mute();
        callAllTags();

        expect(console.log).not.toHaveBeenCalledWith(iLogArgs[0], iLogArgs[1], iLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(dLogArgs[0], dLogArgs[1], dLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(eLogArgs[0], eLogArgs[1], eLogArgs[2]);
    });
});

describe("Tests Log.unmute()", function(){
    var logData;
    var logLocation;

    var iLogArgs;
    var dLogArgs;
    var eLogArgs;

    beforeEach(function(){
        logData = "test log";
        logLocation = "LogHelperTest S3";

        iLogArgs = ["[" + Log.INFO_TAG + "] ", logLocation + " - ", logData];
        dLogArgs = ["[" + Log.DEBUG_TAG + "] ", logLocation + " - ", logData];
        eLogArgs = ["[" + Log.ERROR_TAG + "] ", logLocation + " - ", logData];

        spyOn(console, "log");

        Log.mute();
    });

    afterEach(function(){
        Log.unmute();
    });

    function callAllTags(){
        Log.i(logData, logLocation);
        Log.d(logData, logLocation);
        Log.e(logData, logLocation);
    }

    it("unmutes INFO", function(){
        Log.unmute(Log.INFO_TAG);
        callAllTags();

        expect(console.log).toHaveBeenCalledWith(iLogArgs[0], iLogArgs[1], iLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(dLogArgs[0], dLogArgs[1], dLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(eLogArgs[0], eLogArgs[1], eLogArgs[2]);
    });

    it("unmutes DEBUG", function(){
        Log.unmute(Log.DEBUG_TAG);
        callAllTags();

        expect(console.log).not.toHaveBeenCalledWith(iLogArgs[0], iLogArgs[1], iLogArgs[2]);
        expect(console.log).toHaveBeenCalledWith(dLogArgs[0], dLogArgs[1], dLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(eLogArgs[0], eLogArgs[1], eLogArgs[2]);
    });

    it("unmutes ERROR", function(){
        Log.unmute(Log.ERROR_TAG);
        callAllTags();

        expect(console.log).not.toHaveBeenCalledWith(iLogArgs[0], iLogArgs[1], iLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(dLogArgs[0], dLogArgs[1], dLogArgs[2]);
        expect(console.log).toHaveBeenCalledWith(eLogArgs[0], eLogArgs[1], eLogArgs[2]);
    });

    it("unmutes all", function(){
        Log.unmute();
        callAllTags();

        expect(console.log).toHaveBeenCalledWith(iLogArgs[0], iLogArgs[1], iLogArgs[2]);
        expect(console.log).toHaveBeenCalledWith(dLogArgs[0], dLogArgs[1], dLogArgs[2]);
        expect(console.log).toHaveBeenCalledWith(eLogArgs[0], eLogArgs[1], eLogArgs[2]);
    });

    it("unmutes INFO after specifically muted", function(){
        Log.unmute();
        Log.mute(Log.INFO_TAG);
        Log.mute(Log.DEBUG_TAG);
        Log.mute(Log.ERROR_TAG);
        Log.unmute(Log.INFO_TAG);

        callAllTags();

        expect(console.log).toHaveBeenCalledWith(iLogArgs[0], iLogArgs[1], iLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(dLogArgs[0], dLogArgs[1], dLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(eLogArgs[0], eLogArgs[1], eLogArgs[2]);
    });

    it("unmutes DEBUG after specifically muted", function(){
        Log.unmute();
        Log.mute(Log.INFO_TAG);
        Log.mute(Log.DEBUG_TAG);
        Log.mute(Log.ERROR_TAG);
        Log.unmute(Log.DEBUG_TAG);

        callAllTags();

        expect(console.log).not.toHaveBeenCalledWith(iLogArgs[0], iLogArgs[1], iLogArgs[2]);
        expect(console.log).toHaveBeenCalledWith(dLogArgs[0], dLogArgs[1], dLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(eLogArgs[0], eLogArgs[1], eLogArgs[2]);
    });

    it("unmutes ERROR after specifically muted", function(){
        Log.unmute();
        Log.mute(Log.INFO_TAG);
        Log.mute(Log.DEBUG_TAG);
        Log.mute(Log.ERROR_TAG);
        Log.unmute(Log.ERROR_TAG);

        callAllTags();

        expect(console.log).not.toHaveBeenCalledWith(iLogArgs[0], iLogArgs[1], iLogArgs[2]);
        expect(console.log).not.toHaveBeenCalledWith(dLogArgs[0], dLogArgs[1], dLogArgs[2]);
        expect(console.log).toHaveBeenCalledWith(eLogArgs[0], eLogArgs[1], eLogArgs[2]);
    });
});

describe("Tests Log tag specific functions", function(){
    var goodData: string;
    var goodLocation: string;

    beforeEach(function(){
        goodData = "test log";
        goodLocation = "LogHelperTest S5";

        spyOn(Log, "write");
    });

    it("calls Log.i()", function(){
        Log.i(goodData, goodLocation);

        expect(Log.write).toHaveBeenCalledWith(goodData, Log.INFO_TAG, goodLocation);
    });

    it("calls Log.d()", function(){
        Log.d(goodData, goodLocation);

        expect(Log.write).toHaveBeenCalledWith(goodData, Log.DEBUG_TAG, goodLocation);
    });

    it("calls Log.e()", function(){
        Log.e(goodData, goodLocation);

        expect(Log.write).toHaveBeenCalledWith(goodData, Log.ERROR_TAG, goodLocation);
    });
});
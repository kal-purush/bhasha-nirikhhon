//
// Version 0.0 of bootBot.js, create an environment on/within
//      a Client's HTML browser
//
// This version uses a high-level form of an AJAX command.  It does NOT
// synchronize its various loads of javascript code.  The next version
// should use native AJAX code so that at a clear point in this logic
// the programmer would know if all of the bot's javascript has been
// loaded or not.  Now, the loads occur asynchronously.
//
// A synchronous version of this boot loader is avoided out of politeness
// and a desire to keep from hogging a browser's processor.
//
// This script is the object of a bookmark written as:
//
//    javascript:$.getScript("http:  etc.   ", function(a,b,c) {bootBot("http://localhost/ etc.")};);
//
// The bookmark is an AJAX/jquery request that loads this 
// text and executes it as javascript.  However, since this javascript
// is just a function definition it will NOT execute.
//
// Luckily, getScript has a second parameter which is a callback function that 
// executes ONLY when getScript has successfully completed.
//
// The CAVEAT to all of this is that if getScript loads from an HTTPS resource
// then bootBot must be pointed to javascript stored on an HTTPS resource.
//
// Having to define TWO sources in the bookmark allows the developer to store
// this code on a resource such as //rawgithub and then develop code 
// that resides on //localhost files, OR NOT!  
//
// Either way, this hinge is flexible, enough...
//
// Thus:
//
function bootBot(httpPath: string) {

    //
    // jQuery ver 1.11, so most stuff is possible...
    //alert("jQuery version: " + $.fn.jquery + " is running on this page.");

    // Remember, callbacks don't resolve their parameters UNTIL they
    // are actually invoked.  A cheap way to pass parameters for these
    // callbacks is to store them uniquely in bootBot's namespace.
    //

    //
    // First, load class definitions
    //
    var url1: string  = httpPath + "/PublicClasses.js";
    alert("loading " + url1);
    $.getScript(url1).done(function (a, b, c) { alert(url1 + " is loaded."); })
        .fail(function (a, b, c) { alert(url1 + " has failed to load."); });

    //
    // Second, load user written transactions
    //
    var url2: string = httpPath + "/UserFunctions.js";
    alert("loading " + url2);
    $.getScript(url2).done(function (a, b, c) { alert(url2 + " is loaded."); })
        .fail(function (a, b, c) { alert(url2 + " has failed to load."); });

    //
    // Pentultimate, load control logic
    //
    var url3: string = httpPath + "/Dispatcher.js";
    alert("loading " + url3);
    //$.getScript(url3).done(function (a, b, c) { alert(url3 + " is loaded."); })
    $.getScript(url3).fail(function (a, b, c) { alert(url3 + " has failed to load."); });

    //
    // Finally, load a minimal amount of start-up logic
    //
    var url4 = httpPath + "/Mainline.js";
    alert("loading " + url4);
    //$.getScript(url4).done(function (a, b, c) { alert(url4 + " is loaded."); })
    $.getScript(url4).fail(function (a, b, c) { alert(url4 + " has failed to load."); });

    alert("bootBot has completed, but its callbacks ... probably NOT!");

};  // end of bootBot
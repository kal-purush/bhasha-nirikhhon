/****************************************************************************
	fcoo-moment.js, 

	(c) 2016, FCOO

	https://github.com/FCOO/fcoo-moment
	https://github.com/FCOO

Set-up of common systems, objects, and methods for FCOO web applications use of moment.js and plugins
Sections:
1: Translation for moment-simple-format
2: Add time-zones and translation of there names
4: 
5: 
6: Load format for date, time and timezone from fcoo.settings

****************************************************************************/

(function ($, moment, i18next, window/*, document, undefined*/) {
	"use strict";
	
	//Create fcoo-namespace
	window.fcoo = window.fcoo || {};

    /***********************************************************************
    ************************************************************************
    1: Translation for moment-simple-format
    ************************************************************************
    ***********************************************************************/
    //Add translations for moment-simple-format
    i18next.addPhrases( 'moment', {
        utc     : { en: 'UTC'                },
        local   : { en: 'local', da: 'lokal' },
        dayAbbr : { en: 'd'                  },
        hourAbbr: { en: 'h'    , da: 't'     },
        minAbbr : { en: 'm'                  },
        now     : { en: 'now'  , da: 'nu'    },
        to      : { en: 'to'   , sa: 'til'   }
    });


    //Change language in moment and call sfInit when the language is changed
    window.fcoo.events.on('languagechanged', function(){ 

        //Special case: Norwegian (no) using "Bokmål" (nb)
        moment.locale(i18next.language == 'no' ? 'nb' : i18next.language);

        //Call moment.sfInit( options ) to set text
        moment.sfInit({
           text: {
                utc     : i18next.t('moment:utc'),
                local   : i18next.t('moment:local'),
                dayAbbr : i18next.t('moment:dayAbbr'),
                hourAbbr: i18next.t('moment:hourAbbr'),
                minAbbr : i18next.t('moment:minAbbr'),
                now     : i18next.t('moment:now'),
                to      : i18next.t('moment:to')
           }
       });
    });

    /***********************************************************************
    ************************************************************************
    2: Add time-zones and translation of there names
    ************************************************************************
    ***********************************************************************/
    //Adding the timezones and adding a 'group' options
    moment.sfAddTimezone(
        [
            { id:'Europe/Copenhagen',    group: 'europe' },
            { id:'Europe/London',        group: 'europe' },

            { id:'Atlantic/Faeroe',      group: 'atlantic' },
            { id:'Atlantic/Reykjavik',   group: 'atlantic' },

            { id:'America/Godthab',      group: 'greenland' },
            { id:'America/Scoresbysund', group: 'greenland' },
            { id:'America/Danmarkshavn', group: 'greenland' },
            { id:'America/Thule',        group: 'greenland' }
        ],
        null /* offsetMoment */
    );

    //Add the translation of the timezones incl. the two default id:'local' and id:'utc'
    //Østgrønland (grønlandsk: "Tunu"), Vestgrønland (Grønlandsk: "Kitaa"), Nordgrønland (grønlandsk: "Avannaarsua") 
    i18next.addPhrases( 'timezone', {
        'local'               : { en: 'Local time',                      da: 'Lokaltid'           },
        'utc'                 : { en: 'UTC',                             da: 'UTC'                },
        'Europe/Copenhagen'   : { en: 'Europe/Copenhagen',               da: 'Europa/København'   },
        'Europe/London'       : { en: 'Europe/London',                   da: 'Europa/London'      },
        'Atlantic/Faeroe'     : { en: 'Atlantic/Faeroe Islands',         da: 'Atlanten/Færøerne'  },
        'Atlantic/Reykjavik'  : { en: 'Atlantic/Reykjavik',              da: 'Atlanten/Reykjavik' },
        'America/Godthab'     : { en: 'West Greenland/Nuuk',             da: 'Vestgrøndland/Nuuk',           kl: 'Kitaa/Nuuk'             },
        'America/Scoresbysund': { en: 'East Greenland/Ittoqqortoormiit', da: 'Østgrønland/Ittoqqortoormiit', kl: 'Tunu/Ittoqqortoormiit'  },
        'America/Danmarkshavn': { en: 'East Greenland/Danmarkshavn',     da: 'Østgrønland/Danmarkshavn',     kl: 'Tunu/Danmarkshavn'      },
        'America/Thule'       : { en: 'North Greenland/Thule Air Base',  da: 'Nordgrønland/Thule Air Base',  kl: 'Avannaarsua/Pituffik'   },
    });

    //Translate the names of the timezones when the language is changed
    window.fcoo.events.on('languagechanged', function(){ 
        $.each( moment.simpleFormat.timezoneList, function( index, timezone ){
            timezone.update( i18next.t('timezone:' + timezone.id) );
        });
        
    });


/*        
Standard
    value="local" Local
    value="utc" UTC
Europe
    value="Europe/Copenhagen">Europe / Copenhagen</option>
    value="Europe/London">Europe / London</option>
Atlantic
    value="Atlantic/Faeroe">Faeroe</option>
    value="Atlantic/Reykjavik">Iceland</option>
Greenland
    value="America/Godthab">West Greenland / Nuuk</option>
    value="America/Scoresbysund">East Greenland / Ittoqqortoormiit</option>
    value="America/Danmarkshavn">East Greenland / Danmarkshavn</option>
    value="America/Thule">Thule Air Base</option>
*/



    /***********************************************************************
    ************************************************************************
    6: Load format for date, time and timezone from fcoo.settings
    ************************************************************************
    ***********************************************************************/
    //function to set options in moment.simpleFormat and call global event
    function momentSimpleFormatSetFormat( options ){
        options = $.extend( true, {}, {
                        'date'    : window.fcoo.settings.get('date'),
                        'time'    : window.fcoo.settings.get('time'),
                        'timezone': window.fcoo.settings.get('timezone'),
                    }, 
                    options );
       
        moment.sfSetFormat( options );
        window.fcoo.events.fire('datetimeformatchanged');
    }

    //Set up and load 'date', 'time', and 'timezone' via fcoo.settings
    window.fcoo.settings.add({
        id          : 'date', 
        validator   : function( date ){ return $.inArray( date, ['DMY', 'MDY', 'YMD']) > -1; },
        applyFunc   : function( date ){ momentSimpleFormatSetFormat({ 'date': date });       }, 
        defaultValue: 'DMY',
        callApply   : false
    });
    window.fcoo.settings.add({
        id          : 'time', 
        validator   : function( time ){ return $.inArray( time, ['12', '24']) > -1;    },
        applyFunc   : function( time ){ momentSimpleFormatSetFormat({ 'time': time }); }, 
        defaultValue: '24',
        callApply   : false
    });
    window.fcoo.settings.add({
        id          : 'timezone', 
        validator   : function( timezone ){ return moment.sfGetTimezone( timezone ) !== null;      },
        applyFunc   : function( timezone ){ momentSimpleFormatSetFormat({ 'timezone': timezone }); }, 
        defaultValue: 'local',
        callApply   : false
    });

    //Also fire "datetimeformatchanged" when the language is changed
    window.fcoo.events.on('languagechanged', momentSimpleFormatSetFormat); 


    momentSimpleFormatSetFormat();

    
    /******************************************
	Initialize/ready 
	*******************************************/
	$(function() { 
	
	}); //End of initialize/ready
	//******************************************



}(jQuery, moment, i18next, this, document));
;
/****************************************************************************
    kl.js, 

    Translation of Moment text to Kalaallisut/Greenlandic (language code = kl)

****************************************************************************/

moment.defineLocale('kl', {
    parentLocale: 'da',
  
    /* TODO */



});
module mpp.filter.xlate {

    // data

    var DEFAULT = {
        SPACE: 'space',
        RETURN: 'return',
        BACKSPACE: 'backspace',
        PLAY_PAUSE: 'play/pause',
        SEEK_TO_START: 'seek to start',
        BACKWARD_FORWARD: 'backward/forward',
        ADD_MARKER: 'add marker',
        SEEK_TO_MARKER: 'seek to marker',
        SEEK_TO_ACTIVE_MARKER: 'seek to active marker',
        MOVE_ACTIVE_MARKER: 'move active marker',
        DELETE_MARKER: 'delete marker',
        ADD_SESSION: 'add session',
        ADD_SESSION_MORE: 'add session...',
        EDIT_SESSION: 'edit session',
        DELETE_SESSION: 'delete session',
        ADD: 'add',
        OK: 'ok',
        CANCEL: 'cancel',
        NAME: 'name',
        YT_URL: 'YouTube video url',
        SEARCH_YT: 'search on YouTube',
        SESSION: 'session',
    };

    var GERMAN = {
        SPACE: 'leertaste',
        RETURN: 'eingabetaste',
        BACKSPACE: 'rücktaste',
        LAY_PAUSE: 'play/pause',
        SEEK_TO_START: 'zum anfang springen',
        BACKWARD_FORWARD: 'vorwärts/rückwärts',
        ADD_MARKER: 'marker hinzufügen',
        SEEK_TO_MARKER: 'zum marker springen',
        SEEK_TO_ACTIVE_MARKER: 'zum aktiven marker springen',
        MOVE_ACTIVE_MARKER: 'aktiven marker verschieben',
        DELETE_MARKER: 'marker löschen',
        ADD_SESSION: 'session hinzufügen',
        ADD_SESSION_MORE: 'session hinzufügen...',
        EDIT_SESSION: 'session bearbeiten',
        DELETE_SESSION: 'session löschen',
        ADD: 'hinzufügen',
        OK: 'ok',
        CANCEL: 'abbrechen',
        NAME: 'name',
        YT_URL: 'youtube video url',
        SEARCH_YT: 'auf youtube suchen',
        SESSION: 'session',
    };

    // end data

    var FALLBACK_LANG = 'en';
    var SUPPORTED_LANGS = {
        'en': DEFAULT,
        'de': GERMAN
    };
    var cachedLang: string = undefined;

    function getLang(): string {

        if (cachedLang !== undefined) {
            return cachedLang;
        }

        var lang: string = undefined;
        var nav = <NavigatorExt> window.navigator;

        if (nav.languages) {
            lang = firstSupported(nav.languages);
        }

        if (!lang) {
            lang = firstSupported([nav.userLanguage || nav.language]);
        }
        if (!lang) {
            lang = FALLBACK_LANG;
        }
        cachedLang = lang;
        return lang;
    }

    function firstSupported(languages: string[]): string {
        var first: string = undefined;
        for (var i = 0, n = languages.length; i < n && !first; i++) {
            first = isSupported(languages[i]);
        }
        return first;
    }

    function isSupported(lang: string): string {
        var langKey = lang.toLowerCase().substr(0, 2);
        return SUPPORTED_LANGS[langKey] ? langKey : undefined;
    }

    interface NavigatorExt extends Navigator {
        languages?: string[];
        userLanguage: string;
        browserLanguage: string;
        systemLanguage: string;
    }

    export function filter(k: string): string {
        var lcl = SUPPORTED_LANGS[getLang()];
        return lcl[k] || DEFAULT[k] || k;
        //return lcl[k] || k;
    }
}
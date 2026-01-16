using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AntWars.Helper
{
    class Messages
    {
        public static readonly string OUT_OF_SUGAR = "Es ist kein Zucker mehr verfügbar.";
        public static readonly string OUT_OF_SUGAR_CAPTION = "Zucker ist alle";
        public static readonly string TIME_OUT = "Die Spielzeit von {0} Ticks ist abgelaufen.";
        public static readonly string TIME_OUT_CAPTION = "Zeitüberschreitung";
        public static readonly string PLAYER_WON_TIME_OUT = "{0} hat mit {1} Punkten gewonnen!";
        public static readonly string PLAYER_WON_CAPTION = "Gewonnen";
        public static readonly string PLAYER_WON_MAX_POINTS = "{0} hat die Höchstpunktzahl erreicht!";
        public static readonly string ERROR_INVALID_DLL = "Die DLL ist nicht gültig, bitte probiere eine Andere.";
        public static readonly string ERROR_INVALID_DLL_CAPTION = "Fehler: Ungültige DLL";
        public static readonly string ERROR_COULD_NOT_START = "Es wurden nicht alle Konfigurationen geladen/erstellt.";
        public static readonly string ERROR_COULD_NOT_START_CAPTION = "Fehler: Konfigurationen fehlen";
        public static readonly string ERROR_INVALID_CONFIG = "Die Konfiguration kann nicht geladen werden: \n";
        public static readonly string ERROR_INVALID_CONFIG_CAPTION = "Fehler: Ungültige Konfiguration";
        public static readonly string SAVED = "Erfolgreich gespeichert";
        public static readonly string SAVED_CAPTION = "Gespeichert";
        public static readonly string ERROR_MIN_HIGHER_MAX = "Der Minimalwert darf nicht größer als der Maximalwert sein!";
        public static readonly string ERROR_MIN_HIGHER_MAX_CAPTION = "Fehler: Ungültiger Wert";
    }
}
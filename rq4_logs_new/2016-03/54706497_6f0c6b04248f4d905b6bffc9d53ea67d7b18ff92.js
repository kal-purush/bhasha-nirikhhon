/**
 * Bannière générée automatiquement pour jsdoc
 * Modifier la bannière dans Gruntfile.js concat:options:banner
 */
var bms;
bms = bms || (function() {
    "use strict";

    /////////////////////////////////////////////////////////////////////////////
    // Constantes publiques (déclarés à la fin du script, dans le bloc return) //
    /////////////////////////////////////////////////////////////////////////////

    /**
     * Constantes publiques
     * @type {Object}
     * @property {array} m - mois en texte, version longue
     * @property {array} ma - mois en texte, version courte
     * @property {array} d - jour de la semaine en texte, version longue
     * @property {array} da - jour de la semaine en texte, version courte
     * @public
     */
    var Public_Constants = {
            m: ["janvier", "février", "mars", "avril", "mai", "juin", "juillet", "août", "septembre", "octobre", "novembre", "décembre"],
            ma: ["janv.", "févr.", "mars", "avr.", "mai", "juin", "juil.", "août", "sept.", "oct.", "nov.", "déc."],
            d: ["lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi", "dimanche"],
            da: ["lun.", "mar.", "mer.", "jeu.", "ven.", "sam.", "dim."]
        },

        //////////////////////////////////
        // Méthodes utilitaires privées //
        //////////////////////////////////

        /**
         * Utilitaire : Vérifier si l'argument est une instance de l'objet Date.
         * @memberof  bms
         * @param  {string|number|array|object} un argument
         * @return {boolean}   vrai ou faux
         * @private
         * @since  0.0.1
         */
        id = function(a) {
            return a instanceof Date;
        },
        /**
         * Utilitaire : Récupérer l'année depuis un objet Date.
         * @memberof  bms
         * @param  {object} d un objet `Date`
         * @return {number}   une année sur 4 chiffres
         * @private
         * @since  0.0.1
         */
        gy = function(d) {
            return d.getFullYear();
        },
        /**
         * Utilitaire : Récupérer le mois depuis un objet Date.
         * @memberof  bms
         * @param  {object} d un objet `Date`
         * @return {number}   un mois sur 2 chiffres (1-12)
         * @private
         * @since  0.0.1
         */
        gm = function(d) {
            return d.getMonth();
        },
        /**
         * Utilitaire : Récupérer le jour depuis un objet Date.
         * @memberof  bms
         * @param  {object} d un objet `Date`
         * @return {number}   un jour sur 2 chiffres (1-31)
         * @private
         * @since  0.0.1
         */
        gd = function(d) {
            return d.getDate();
        },
        /**
         * Utilitaire : Ajouter un préfixe 0 à un chiffre.
         * @memberof  bms
         * @param  {number} n un chiffre (0-9)
         * @return {string|number}   un Nombre sur 2 chiffres (00-09)
         * @private
         * @since  0.0.1
         */
        az = function(n) {
            return (n < 10) ? "0" + n : n;
        },

        ///////////////////////////////////////////////////////////////////////////
        // méthodes publiques (déclarés à la fin du script, dans le bloc return) //
        ///////////////////////////////////////////////////////////////////////////

        /**
         * Méthode pour créer un objet javascript `Date` à partir d'une date du calendrier grégorien français.
         * @memberof  bms
         * @public
         * @since  0.0.0
         * @param  {number} dd   jour sur 2 chiffres (01-31)
         * @param  {number} mm   mois sur 2 chiffres (01-12)
         * @param  {number} yyyy année sur 4 chiffres
         * @return {object}      un objet `Date`
         * @example <pre>
<strong>> bms.d(21, 4, 2014);</strong>
<em>Date {Mon Apr 21 2014 00:00:00 GMT+0200 (CEST)}</em>
</pre>
         */
        d = function(dd, mm, yyyy) {
            return new Date(yyyy, mm - 1, dd);
        },

        /**
         * Méthode pour convertir un objet `Date` en objet `Calendrier` (sans l'heure) au format grégorien. La nomenclature est inspirée de la fonction PHP `date()`.
         * @memberof  bms
         * @public
         * @since 0.0.1
         * @param  {object} d   un objet `Date`
         * @returns {object.<number, string>} un objet `Calendrier Grégorien`,<br>
         * @returns {number} Y - année sur 4 chiffres,<br>
         * @returns {number} y - année sur 2 chiffres,<br>
         * @returns {number} n - mois en chiffres sans le zéro (1-12),<br>
         * @returns {number} m - mois en chiffres avec le zéro  (01-12),<br>
         * @returns {string} F - mois en texte, version longue,<br>
         * @returns {string} M - mois en texte, version courte,<br>
         * @returns {number} j - jour du mois en chiffres sans le zéro (1-31),<br>
         * @returns {number} d - jour du mois en chiffres avec le zéro (01-31),<br>
         * @returns {number} N - jour de la semaine en chiffres au format ISO-8601 (1 pour Dimanche, 7 pour Samedi),<br>
         * @returns {number} w - jour de la semaine en chiffres (0 pour Dimanche, 6 pour Samedi),<br>
         * @returns {string} l - jour de la semaine en texte, version longue,<br>
         * @returns {string} D - jour de la semaine en texte, version courte.
         * @example <pre>
<strong>> var date = bms.d(21,4,2014);
> bms.cg(date);</strong>
<em>Object {
    D = "lun."
    F = "avril"
    M = "avr."
    N = 1
    Y = 2014
    d = 21
    j = 21
    l = "lundi"
    m = "04"
    n = 4
    w = 0
    y = 14
}</em>
<strong>> bms.cg(date).Y;</strong>
<em>2014</em>
</pre>
         */
        cg = function(d) {
            if (id(d)) {
                var Y = gy(d),
                    n = gm(d) + 1,
                    j = gd(d),
                    N = (d.getDay() === 0) ? 7 : d.getDay(),
                    w = N - 1;
                return {
                    Y: Y,
                    y: parseInt(Y.toString().substr((Y.toString().length) - 2, 2), 10),
                    n: n,
                    m: az(n),
                    F: Public_Constants.m[n - 1],
                    M: Public_Constants.ma[n - 1],
                    j: j,
                    d: az(j),
                    N: N,
                    w: w,
                    l: Public_Constants.d[w],
                    D: Public_Constants.da[w]
                };
            }
        },

        /**
         * Méthode pour formater un objet `Calendrier` en texte lisible, à partir d'un masque.
         * @memberof  bms
         * @public
         * @param  {object} d   un objet `Date`
         * @param  {string} [format] un masque défini par l'utilisateur (optionnel) et correspondant aux propriétés de l'objet `Calendrier`
         * @return {string}     une date formatée
         * @since 0.0.1
         * @example <pre>
<strong>> var date = bms.d(21,4,2014);
> var greg = bms.cg(date);
> bms.cf(greg);</strong>
<em>"21 avril 2014"</em>
<strong>> bms.cf(greg,"{l} {J} {F} {Y}");</strong>
<em>"lundi 21 avril 2014"</em>
<strong>> bms.cf(greg,"{J}/{m}/{Y}");</strong>
<em>"21/04/2014"</em>
</pre>
         */
        cf = function(d, format) {
            var p, reg, map = {
                Y: d.Y,
                y: d.y,
                n: d.n,
                m: d.m,
                F: d.F,
                M: d.M,
                j: d.j,
                d: d.d,
                N: d.N,
                w: d.w,
                l: d.l,
                D: d.D
            };
            if (d.Y) {
                if (!format) {
                    format = "{j} {F} {Y}";
                }
                for (p in map) {
                    if (map.hasOwnProperty(p)) {
                        reg = new RegExp("{" + p + "}", "g");
                        format = format.replace(reg, map[p]);
                    }
                }
            } else {
                format = "";
            }
            return format;
        },

        /**
         * Méthode pour créer un objet `Acte` à partir de la date de l'acte et des ages mentionnés dans l'acte.
         * @memberof  bms
         * @public
         * @since  0.0.0
         * @param  {object} d   un objet `Date`
         * @param  {number} age un age mentionné sur l'acte
         * @returns {object.<object,number>} un objet `Acte`<br>
         * @returns {object} s - la date de naissance minimale<br>
         * @returns {object} e - la date de naissance maximale<br>
         * @returns {number} y - l'année de naissance sur 4 chiffres
         * @example <pre>
<strong>> var date = bms.d(21, 4, 2016);
> bms.a(date, 2);</strong>
<em>Object {
   s=Date {Mon Apr 22 2013 00:00:00 GMT+0200 (CEST)},
   e=Date {Mon Apr 20 2015 00:00:00 GMT+0200 (CEST)},
   y=2014
}</em>
<strong>> bms.a(date, 2).y;</strong>
<em>2014</em>
</pre>
         */
        a = function(d, age) {
            if ((id(d)) && (isFinite(parseInt(age, 10)))) {
                return {
                    "s": new Date(gy(d) - 1 - age, gm(d), gd(d) + 1),
                    "e": new Date(gy(d) + 1 - age, gm(d), gd(d) - 1),
                    "y": parseInt(gy(d) - age, 10)
                };
            }
        },

        /**
        * Méthode pour créer un objet `Période`contenant le nombre de jours entre 2 objets `Date`.
        * @memberof  bms
        * @public
        * @since  0.0.0
        * @param  {object} s un objet `Date` (de début)
        * @param  {object} e un objet `Date` (de fin)
        * @returns {object.<object,number>} un objet `Période`<br>
        * @returns {object} s - un objet `Date` de départ (le plus petit)<br>
        * @returns {object} e - un objet `Date` de fin (le plus grand)<br>
        * @returns {number} l - la durée de la période en jours
        * @example <pre>
<strong>> var date1 = bms.d(20, 4, 2015);</strong>
<strong>> var date2 = bms.d(22, 4, 2013);</strong>
<strong>> bms.p(date1, date2);</strong>
<em>Object {
    s = Date {Mon Apr 22 2013 00:00:00 GMT+0200 (CEST)},
    e = Date {Mon Apr 20 2015 00:00:00 GMT+0200 (CEST)},
    l = 729
}</em>
<strong>> bms.p(date1, date2).l</strong>
<em>729</em>
</pre>
        */
        p = function(s, e) {
            if ((id(s)) && (id(e))) {
                return {
                    "s": (s > e) ? e : s,
                    "e": (s > e) ? s : e,
                    "l": Math.round(Math.abs((e - s) / (1000 * 60 * 60 * 24)) + 1)
                };
            }
        },

        /**
        * Méthode pour créer un objet `Chevauchement` contenant le nombre de jours communs à 2 objets `Période`.
        * @memberof  bms
        * @public
        * @since  0.0.0
        * @param  {object} p1 un premier objet `Période`ou `Acte`
        * @param  {object} p2 un second objet `Période`ou `Acte`
        * @returns {object.<object,number>} un objet `Chevauchement`<br>
        * @returns {object} s - un objet `Date` de départ du chevauchement (le plus petit)<br>
        * @returns {object} e - un objet `Date` de fin du chevauchement (le plus grand)<br>
        * @returns {number} l - la durée du chevauchement en jours
        * @example <pre>
<strong>> var date1 = bms.d(20, 4, 2015);
> var date2 = bms.d(22, 4, 2013);
> var date3 = bms.d(20, 4, 2015);
> var periode1 = bms.p(date1, date2);
> var acte1 = bms.a(date3, 2);
> bms.po(periode1, acte1);</strong>
<em>Object {
    s = Date {Mon Apr 22 2013 00:00:00 GMT+0200 (CEST)},
    e = Date {Sat Apr 19 2014 00:00:00 GMT+0200 (CEST)},
    l = 363
}</em>
<strong>> bms.po(periode1, acte1).l;</strong>
<em>363</em>
</pre>
        */
        po = function(p1, p2) {
            var overlap, min, max;
            if (p1 && p2 && (id(p1.s)) && (id(p1.e)) && (id(p2.s)) && (id(p2.e))) {
                if ((p1.e < p2.s) || (p1.s > p2.e)) {
                    overlap = {};
                } else {
                    min = new Date(Math.max(p1.s, p2.s));
                    max = new Date(Math.min(p1.e, p2.e));
                    overlap = p(min, max);
                }
            }
            return overlap;
        },
        /**
         * Méthode pour formater un objet `Date` (sans l'heure) en texte lisible.<br>
Cette méthode a été remplacée par [bms.cf(d, [format])](#bms.cf).
         * @deprecated en version 0.0.1, replacée par `bms.cf`, à retirer en version 1.0.0
         * @memberof  bms
         * @public
         * @since 0.0.0
         * @param  {object} d   un objet `Date`
         * @param  {string} [sep] un caractère de séparation (optionnel)
         * @return {string}     une date formatée
         * @example <pre>
<strong>> var date = bms.d(21, 4, 2014);
> bms.df(date);</strong>
<em>"21 avril 2014"</em>
<strong>> bms.df(date, '/');</strong>
<em>"21/4/2014"</em>
<strong>> bms.df(date, '-');</strong>
<em>"21-4-2014"</em>
</pre>
         */
        df = function(d, sep) {
            if (id(d)) {
                return (sep === undefined) ? gd(d) + " " + Public_Constants.m[gm(d)] + " " + (gy(d)) : gd(d) + sep + (gm(d) + 1) + sep + (gy(d));
            }
        };

    /////////////////////////
    // Public declarations //
    /////////////////////////

    return {
        c: Public_Constants,
        d: d,
        cg: cg,
        cf: cf,
        df: df,
        a: a,
        p: p,
        po: po
    };
}());
// https://meumobi.github.io/jekyll/2019/06/05/multi-languages-with-jekyll.html
// Detect browser language
var [lang, locale] = (((navigator.userLanguage || navigator.language).replace('-', '_')).toLowerCase()).split('_');

// N.B.: I'm hard-coding this part until I figure out a better way. --Pedro
var supported = ['en', 'fr', 'pt']

/*
 * Auto switcher
 *
 * The auto-switcher is responsible for defining the best suitable lang
 * supported for browser lang and when redirect user to it.
 *
 * Best suitable lang supported
 *
 * We can detect the best suitable lang supported as done below. We check first
 * if we can satisfy the lang and locale (or region), if not we check for only
 * lang. For instance, if we support en and browser lang is en-US, our function
 * will return en. If none is supported the function return the default lang
 * (1st of supported array).
 */
function getBestSuitableSupportedLang(lang, locale, supported) {
  // Exclude first element, default language
  var supported_lang = supported.shift();
  
  if (supported.includes(lang + "-" + locale)) {
    supported_lang = lang + "-" + locale;
  } else if (supported.includes(lang)) {
    supported_lang = lang;
  }

  return supported_lang;
}
/*
 * NB: we use includes method on array, that is not supported by IE, but who
 * cares about IE ?…
 */

/*
 * Redirect
 *
 * Then, if current lang of site is different of best suitable returned by
 * function getBestSuitableSupportedLang means we should redirect user. And
 * should works as described below:
 */
//var [lang, locale] = (((navigator.userLanguage || navigator.language).replace('-', '_')).toLowerCase()).split('_');
//var supported_languages = ['en', 'fr', 'pt'];
var current_lang = document.documentElement.lang;

var suitable_lang = getBestSuitableSupportedLang(lang, locale, supported)

/*
 * Disable this since we need to check landing page, further below --Pedro
 *
 *if (current_lang !== suitable_lang) {
 *  window.location = '/' + suitable_lang + '/';
 *}
 */

/*
 * Check if landing page
 *
 * But the auto-switcher to be compliant with static-switcher we should add a
 * mechanism to detect if user choose the language, if yes then disable
 * auto-switcher. To achieve it we check if user is landing on our site, if yes
 * enable auto-switcher if not means user choose a different language and we
 * disable auto-switcher.
 */
var hostname = window.location.hostname;
var referrer = document.referrer;

var landingPage = !referrer || referrer.indexOf(hostname) == -1;

if (landingPage && (current_lang !== suitable_lang)) {
  window.location = '/' + suitable_lang + '/';
}
/*
 * Redirect only home
 *
 * And because all pages not necessarly are translated we’ll enable
 * auto-switcher ONLY on home. For instance, if user access directly the page
 * /en/about we’ll not redirect him to /pt/sobre (user could use
 * static-switcher to achieve it). This is done by including the auto-switcher
 * on page only for index.html, using a specific layout for it or a conditional
 * include.
 *
 * N.B.: Taken care of by Minimal-Mistakes 'page_js' frontmatter. --Pedro
 */
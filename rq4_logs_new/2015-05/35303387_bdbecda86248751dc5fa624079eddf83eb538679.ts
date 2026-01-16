// Konfiguration des Testsystems
//config.baseURL = http://www.epoche-napoleon.net/
#config.admPanel = 0
#config.debug = 0
#entwicklung = 0

// Konfiguration des Echtsystems
entwicklung = 1
config.admPanel = 1
config.debug = 1
config.baseURL = http://p238712.mittwaldserver.info/

# Systemverzeichnisse
FLUID = fileadmin/project/Fluidtemplate/
PARTIAL = fileadmin/project/Partials/
LAYOUT = fileadmin/project/Layout/
EXT = fileadmin/project/EXT/
CSS = fileadmin/project/css/
BILD = fileadmin/project/bild/

#Systemvarialbeln:
abcmenushow = 0

# Entwicklungsvariable
entwicklung = 0

# AdminPanal
admPanal = 1

# SocialsharePrivacy
plugin.tx_socialshareprivacy_pi1.javascript_in_footer = 1



# METASEO
plugin.metaseo {
  metaTags {
    copyright = (c) Projekt EPOCHE NAPOLEON by Michael Gnessner 2003 -  %YEAR%
    revisit = 14
    wotVerification =
    googleVerification = 7c3144f39dce9812
    msnVerification = 679FD8DF6EB8F320124AB087180A6812
    author = Michael Gnessner
    publisher = © Projekt EPOCHE NAPOLEON 2003-2014
    language = de
    geoRegion = DE-NW
    rating = General
    email = redaktion@epoche-napoleon.net
  }

  pageTitle {
    applySitetitleToPagetitle = 0
    sitetitlePosition = 1
    sitetitle = EPOCHE NAPOLEON
    sitetitleGlueSpaceBefore = 1
    sitetitleGlueSpaceAfter = 1
  }

  services {
    googleAnalytics = UA-50240478-1
    googleAnalytics.domainName = auto
  }
  social.googleplus.profilePageId = 106636556970977419778
}

plugin.tt_news {
  templateFile = fileadmin/project/TMPL/news_tmpl.html
  pid_list = 19
  singlePid = 28
  archiveTypoLink = 27

  // Bildgröße in Einzelansicht
  singleMaxW = 350
  singleMaxH = 350
  
  // Anzahl der Nachrichten im LastView
  limit = 10
  lastLimit = 5

  showTitleAsPrevNextLink = 0
}
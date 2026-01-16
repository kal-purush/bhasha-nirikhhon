lib.content = CONTENT
lib.content.table = tt_content
lib.content.select {
	# "sorting" is a column from the tt_content table and
	# keeps track of the sorting order which is given in the backend
	orderBy = sorting
	languageField = sys_language_uid
	# header column
	where.field = colPos
	where.wrap = colPos = |
}

### ------ Fluid Template --------
page.10 = FLUIDTEMPLATE
page.10 {
	file = fileadmin/layout/ffpi/templates/default.html
	layoutRootPath = fileadmin/layout/ffpi/templates/layouts/
	partialRootPath = fileadmin/layout/ffpi/templates/partials/

	variables {
		NAVIGATION < lib.navigation.10
	}
}

### --------- CSS -----------
page.includeCSS.main = fileadmin/layout/ffpi/css/default.css
page.includeCSS.main.media = all


###################
# Content Elemente
###################

# Bilder sollen keine Breite oder Höhe bekommen
tt_content.image.20.stdWrap.parseFunc.nonTypoTagStdWrap.HTMLparser.tags.img.fixAttrib {
    width.unset = 1
    height.unset = 1
    style.unset = 1
}
tt_content.textpic.20.stdWrap.parseFunc.nonTypoTagStdWrap.HTMLparser.tags.img.fixAttrib {
    width.unset = 1
    height.unset = 1
    style.unset = 1
}
lib.parseFunc_RTE.nonTypoTagStdWrap.HTMLparser.tags.img.fixAttrib {
    width.unset = 1
    height.unset = 1
    style.unset = 1
}

# default css entfernen
plugin.tx_cssstyledcontent._CSS_DEFAULT_STYLE >
plugin.tx_felogin_pi1._CSS_DEFAULT_STYLE >
plugin.tx_indexedsearch._CSS_DEFAULT_STYLE >
plugin.tx_form._CSS_DEFAULT_STYLE >

# Content Header als H2
content.defaultHeaderType = 2

#Überschriften von Klassen Attribut befreien
lib.stdheader {
    10.1.fontTag = |
    10.2.fontTag = |
    10.3.fontTag = |
    10.4.fontTag = |
    10.5.fontTag = |
    stdWrap.dataWrap = |
}

#csc-header entfernen
lib.stdheader.stdWrap.dataWrap >

#Überschriften ohne Zusätze
lib.stdheader.2.headerStyle >
lib.stdheader.3.headerClass >

#Klassen Attribut aus P-Tag entfernen
lib.parseFunc_RTE.nonTypoTagStdWrap.encapsLines.addAttributes.P.class >

#Sitemap Element für blog Posting
tt_content.menu.20.4 >

tt_content.menu.20.4 < tt_content.menu.20.1
tt_content.menu.20.4.stdWrap.outerWrap = <div class="bloglist">|</div>

tt_content.menu.20.4.1.wrap >

tt_content.menu.20.4.1.NO.wrapItemAndSub = |
tt_content.menu.20.4.1.NO {
  linkWrap = |
  allWrap = <section class="bloglist-item" itemscope itemtype="http://schema.org/BlogPosting">|</section>
  ATagBeforeWrap = 0
  doNotLinkIt = 1
  stdWrap.htmlSpecialChars = 0
  stdWrap.typolink.parameter.field = uid
  stdWrap.cObject = COA
  stdWrap.cObject {
    # Untertitel 
    20 = TEXT
    20 {
      field = crdate
      strftime = %d.%m.%Y
      wrap = <span class="date" itemprop="dateCreated">|</span>
    }
		# Titel 
    10 = TEXT
    10 {
      field = title
      wrap = <h4 class="title">|</h4>
      
    }
    # Inhaltsangabe (Abstract)
    30 = TEXT
    30 {
      field = abstract
      htmlSpecialChars = 1
      wrap = <div class="text" itemprop="description">|</div>
    }
  }
}

# Typo3 Layout Optionen Content-Elemente
    tt_content.stdWrap.innerWrap.cObject = CASE
    tt_content.stdWrap.innerWrap.cObject {
        key.field = layout
        1 = TEXT
        1.value = <div class="csc-default big-box">|</div>

        2 = TEXT
        2.value = <div class="csc-default small-box">|</div>
    }
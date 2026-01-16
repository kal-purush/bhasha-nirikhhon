# ***************************************************************************
# Reused parts form file ../../../typo3/sysext/css_styled_content/static/v6.1/setup.txt Configuration/TypoScript/Elements/sitemp.ts
# To style more complicated structures
# ***************************************************************************

# ****************
# CType: menu
# ****************
tt_content.menu = COA
tt_content.menu {

	20 = CASE
	20 {

		# "Menu of these pages"
		default = HMENU
		default {
			stdWrap < tt_content.menu.20.default.stdWrap
			stdWrap {
				outerWrap = <div class="main__textblock">|</div>
			}
			1 = TMENU
			1 {
				target < lib.parseTarget
				target =
				target.override = {$PAGE_TARGET}
				expAll = 1
				wrap = <ul class="menu__list">|</ul>
				NO {
					ATagTitle.field = description // title
					ATagParams = class="menu__link"
				}
				IFSUB = 1
				IFSUB {
					ATagParams = class="menu__link menu__link--has-children"
				}
				CUR = 1
				CUR {
					ATagParams = class="menu__link menu__link--active"
				}
				CURIFSUB = 1
				CURIFSUB {
					ATagParams = class="menu__link menu__link--active menu__link--has-children"
				}
			}
			2 < .1
			2.wrap = <ul class="menu__list menu__list--indented">|</ul>
			3 < .2
			4 < .2
			5 < .2
			6 < .2
			7 < .2
		}
		# "All other menues"
		1.stdWrap < .default.stdWrap
		1.1 < .default.1
		1.2 < .default.2
		1.3 < .default.2
		1.4 < .default.2
		1.5 < .default.2
		1.6 < .default.2
		1.7 < .default.2
		2.stdWrap < .default.stdWrap
		2.1 < .default.1
		2.2 < .default.2
		2.3 < .default.2
		2.4 < .default.2
		2.5 < .default.2
		2.6 < .default.2
		2.7 < .default.2
		3.stdWrap < .default.stdWrap
		3.1 < .default.1
		3.2 < .default.2
		3.3 < .default.2
		3.4 < .default.2
		3.5 < .default.2
		3.6 < .default.2
		3.7 < .default.2
		4.stdWrap < .default.stdWrap
		4.1 < .default.1
		4.2 < .default.2
		4.3 < .default.2
		4.4 < .default.2
		4.5 < .default.2
		4.6 < .default.2
		4.7 < .default.2
		5.stdWrap < .default.stdWrap
		5.1 < .default.1
		5.2 < .default.2
		5.3 < .default.2
		5.4 < .default.2
		5.5 < .default.2
		5.6 < .default.2
		5.7 < .default.2
		6.stdWrap < .default.stdWrap
		6.1 < .default.1
		6.2 < .default.2
		6.3 < .default.2
		6.4 < .default.2
		6.5 < .default.2
		6.6 < .default.2
		6.7 < .default.2
		7.stdWrap < .default.stdWrap
		7.1 < .default.1
		7.2 < .default.2
		7.3 < .default.2
		7.4 < .default.2
		7.5 < .default.2
		7.6 < .default.2
		7.7 < .default.2
		8.stdWrap < .default.stdWrap
		8.1 < .default.1
		8.2 < .default.2
		8.3 < .default.2
		8.4 < .default.2
		8.5 < .default.2
		8.6 < .default.2
		8.7 < .default.2
	}
}
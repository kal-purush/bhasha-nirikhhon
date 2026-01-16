tt_content.image.20 {

	# remove and change standard wrap csc-textpic csc-textpic-responsive...
	layout = CASE
	layout {
		# remove <div class="csc-textpic csc-textpic-responsive ...></div>
		key.field = imageorient
		default = TEXT
		default.value =
		default.override = ###IMAGES######TEXT###
		1 < .default
		2 < .default
		8 < .default
		9 < .default
		10 < .default
		17 < .default
		18 < .default
		25 < .default
		26 < .default
	}

	# remove also csc-textpic-imagewrap, csc-textpic-center-outer and csc-textpic-center-inner
	# remove default picture tag create new one to include caption into <picture>-tag
	rendering {
		singleNoCaption {
			# Single image - No caption
			allStdWrap.innerWrap.cObject.0.value = |
			allStdWrap.dataWrap.override = |
			allStdWrap.innerWrap.cObject.setCurrent.0.value =
			allStdWrap.innerWrap.cObject.setCurrent.8.value =
			singleStdWrap.wrap.override = <picture>|</picture>
		}
		noCaption {
			# Multiple images and no caption at all
			allStdWrap.dataWrap.override = |
			singleStdWrap.wrap.override = --><picture>|</picture><!--
			rowStdWrap.wrap.override = |
			columnStdWrap.wrap.override = |
			noRowsStdWrap.wrap.override = |
			lastRowStdWrap.wrap.override = |
			allStdWrap.wrap.override = <div class="main__gallery"><!--|--></div>
		}
		singleCaption {
			# Just one image with a caption
			singleStdWrap.wrap.override = <picture>|###CAPTION###</picture>
			caption.wrap.override = <div>|</div>
		}
		splitCaption {
			# Several images???
			singleStdWrap.wrap.override = --><picture>|###CAPTION###</picture><!--
			rowStdWrap.wrap.override = |
			noRowsStdWrap.wrap.override = |
			noColsStdWrap.wrap.override = |
			lastRowStdWrap.wrap.override = |
			columnStdWrap.wrap.override = |
			caption.wrap.override = <div>|</div>
			allStdWrap.wrap = <div class="main__gallery"><!--|--></div>
		}
		globalCaption {
			# Just one image without a caption
			allStdWrap.dataWrap.override = |
			singleStdWrap.wrap.override = <picture>|</picture>
		}
	}

	1.layoutKey >
	1.layoutKey = picturefill

	1.layout.picturefill {
		element = <!--[if IE 9]><video style="display: none;"><![endif]-->###SOURCECOLLECTION###<img srcset="###SRC###" ###PARAMS### ###ALTPARAMS######SELFCLOSINGTAGSLASH### /><!--[if IE 9]></video><![endif]-->
		source = <source srcset="###SRC###" media="###MEDIAQUERY###"></source>
	}

	maxW >
	maxWInText >
	#maxWInText = 375
	maxW = 480

	1.sourceCollection >
	1.sourceCollection {

		mini {
			width = 480
			maxW = 480
			mediaQuery = (max-width: 480px)
			srcsetCandidate = 480w
			dataKey = mini
		}

		small {
			width = 768
			maxW = 768
			mediaQuery = (max-width: 768px)
			srcsetCandidate = 768w
			dataKey = small
		}

		regular {
			width = 1024
			maxW = 1024
			pixelDensity = 2
			mediaQuery = (max-width: 1024px)
			srcsetCandidate = 1024w 2x
			dataKey = regular
		}

		wide {
			width = 1200
			maxW = 1200
			mediaQuery = (max-width: 1200px)
			srcsetCandidate = 1200w
			dataKey = wide
		}

		gigantic {
			width =
			maxW =
			srcsetCandidate =
			mediaQuery = (min-width: 1201px)
			dataKey = gigantic
		}
	}
}
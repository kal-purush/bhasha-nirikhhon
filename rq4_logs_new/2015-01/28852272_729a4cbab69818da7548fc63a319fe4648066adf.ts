mod.SHARED.defaultLanguageFlag = dede

// Audiocontentelement zu "new content element wizzard" hinzufügen
mod.wizards.newContentElement.wizardItems.common {
	elements.fluidcontentelement {
		icon = gfx/c_wiz/multimedia.gif
		title = Audioelment
		description = Show Audio Content
		tt_content_defValues.CType = textpic
	}
	show := addToList(fluidcontentelement)
}

TCEMAIN {
	permissions {
		# Besitzergruppe (ID der Gruppe Page Access):
		groupid = 2
		# Rechte Besitzer:
		user = show, editcontent, edit, delete, new
		# Rechte Besitzergruppe:
		group = show
		# Rechte "everybody" (kann ungesetzt bleiben):
		everybody =
	}
}

// Standardmäßig sollen Clipboard, Localization und Editiermöglichkeiten im Backend angezeigt werden
mod.web_list {
	enableClipBoard = activated
	enableDisplayBigControlPanel = activated
	enableLocalizationView = activated

	// verstecke Elemente die im "new record wizard" neu angelegt werden könnten
	deniedNewTables = tx_devlog,backend_layout,sys_domain,tx_rtehtmlarea_acronym,sys_template,tx_scheduler_task_group,sys_note,sys_file_collection,tx_beacl_acl
}

[TS][usergroup = {$ids.reducedRightsEditors}]
// Setzen der Benutzerrechte beim Anlegen von Seiten und Inhalt
// TODO: muss noch durch Variablen gesteuert werden


// Verstecken unerwünschter Seiten- und Inhaltselemente
TCEFORM {
	// Entfernen des alternativen Text von image
	// Todo: würde es vermutlich überall entfernen
	#sys_file_reference.alternative.disabled  = 1

	pages {
		description.disabled = 0
		media.disabled = 0
		author.disabled = 0
		doktype.removeItems = spacer

	}

	tt_content {
		// Unerwünschte Seitentypen ausblenden (sowohl im "new content element wizard" als auch im Menü)
		// sollen text, image,table, filelist, sitemap(menu), plugin und eigene Elemente bleiben
		CType.removeItems = header,textpic,bullets,media,shortcut,html,div,multimedia,mailform,login,search,tx_beacl_acl

		// Unerwünschte Auswahlmöglichkeiten bei Inhaltselementen entfernen
		header_layout.removeItems = 1,2,3,4,5
		header_position.disabled = 1
		colPos.disabled = 1
		header_link.disabled = 1
		date.disabled = 1
		linkToTop.disabled = 1
		fe_group.disabled = 1
		section_frame.disabled = 1
		layout.disabled = 1
		margin_top.disabled = 1
		sectionIndex.disabled = 1
		spaceAfter.disabled = 1
		spaceBefore.disabled = 1
		// image:
		imageheight.disabled = 1
		imagewidth.disabled = 1
		imageborder.disabled = 1
		image_effects.disabled = 1
		imageorient.disabled = 1
		image_compression.disabled = 1
		image_noRows.disabled = 1
		imagecols.disabled = 1
		imagecaption_position.disabled = 1
		longdescURL.disabled = 1
		image_zoom.disabled = 1
		// table
		pi_flexform.table.sDEF {
			acctables_tfoot.disabled = 1
			acctables_nostyles.disabled = 1
			acctables_headerpos.disabled = 1
			acctables_tableclass.disabled = 1
		}
	}
}
// steht in der localconf zum Enternen des Link zum direkten Upload im Contentelement:
setup.override.edit_docModuleUpload = 0


// Im "new content element wizard" keine Tabs anzeigen, es sind sowieso nicht so viele Elemente
mod.wizards.newContentElement.renderMode = list


// Configuration RTE
RTE.default {
	hideButtons(
		class,
		blockstylelabel,
		blockstyle,
		textstylelabel,
		textstyle,
		indent,
		outdent,
		textindicator,
		table,
		toggleborders,
		tableproperties,
		chMode,
		rowproperties,
		rowinsertabove,
		rowinsertunder,
		rowdelete,
		rowsplit,
		columninsertbefore,
		columninsertafter,
		columndelete,
		columnsplit,
		cellproperties,
		cellinsertbefore,
		cellinsertafter,
		celldelete,
		cellsplit,
		cellmerge,
		underline,
		strikethrough
	)
	buttons.formatblock.removeItems (
		address,
		article,
		aside,
		div,
		footer,
		nav,
		header,
		h1,
		h2,
		h5,
		h6,
		pre,
		section
	)
}

[global][/TS]
/**
* Verschiedene Aktionen für die Buttons
*
* @since   	3.2.10
* @changed  3.4.8
*/

function wpAppbox_amazonapps_button() {
	return "[appbox amazonapps " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_appstore_button() {
	return "[appbox appstore " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_chromewebstore_button() {
	return "[appbox chromewebstore " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_firefoxaddon_button() {
	return "[appbox firefoxaddon " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_firefoxmarketplace_button() {
	return "[appbox firefoxmarketplace " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_goodoldgames_button() {
	return "[appbox goodoldgames " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_googleplay_button() {
	return "[appbox googleplay " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_itunes_button() {
	return "[appbox itunes " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_operaaddons_button() {
	return "[appbox operaaddons " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_steam_button() {
	return "[appbox steam " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_windowsstore_button() {
	return "[appbox windowsstore " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_wordpress_button() {
	return "[appbox wordpress " + tinyMCE.activeEditor.selection.getContent() + "]";
}

function wpAppbox_xda_button() {
	return "[appbox xda " + tinyMCE.activeEditor.selection.getContent() + "]";
}


/**
* WP-Appbox-Button für TinyMCE
*
* @since   3.2.10
* @changed  3.4.8
*/

if ( typeof wpappbox_combined_button != 'undefined' ) {
	var button_store_ids = wpappbox_combined_button['ids'],
		button_store_names = wpappbox_combined_button['names']
		values = [],
		i = 0;
	for (i = 0; i < button_store_ids.length; i++) {
		if ( button_store_ids[i] == 'amazonapps' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_amazonapps_button()); } } );
		}
		if ( button_store_ids[i] == 'appstore' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_appstore_button()); } } );
		}
		if ( button_store_ids[i] == 'chromewebstore' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_chromewebstore_button()); } } );
		}
		if ( button_store_ids[i] == 'firefoxaddon' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_firefoxaddon_button()); } } );
		}
		if ( button_store_ids[i] == 'firefoxmarketplace' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_firefoxmarketplace_button()); } } );
		}
		if ( button_store_ids[i] == 'goodoldgames' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_goodoldgames_button()); } } );
		}
		if ( button_store_ids[i] == 'googleplay' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_googleplay_button()); } } );
		}
		if ( button_store_ids[i] == 'operaaddons' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_operaaddons_button()); } } );
		}
		if ( button_store_ids[i] == 'steam' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_steam_button()); } } );
		}
		if ( button_store_ids[i] == 'windowsstore' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_windowsstore_button()); } } );
		}
		if ( button_store_ids[i] == 'wordpress' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_wordpress_button()); } } );
		}
		if ( button_store_ids[i] == 'xda' ) {
			values.push( { text: button_store_names[i], onclick : function() { tinyMCE.activeEditor.execCommand('mceInsertContent', false, wpAppbox_xda_button()); } } );
		}
	}
	tinymce.PluginManager.add('wpAppbox_CombinedButton', function(editor, url) {
		editor.addButton('wpAppbox_AppboxButton', {
			type: 'menubutton',
			icon: 'icon wpappbox-tinymce-button',
			image : '../wp-content/plugins/wp-appbox/buttons/appbox.btn.png',
			menu : values
		});
	});
}
	

/**
* Die einzelnen Buttons für den TinyMCE
*
* @since   3.2.10
* @changed  3.4.8
*/

tinymce.create('tinymce.plugins.wpAppbox_StoreButtons', {
	init : function(ed, url) {
		
		ed.addButton('wpAppbox_AmazonAppsButton', {
			title : 'Amazon Apps Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_amazonapps_button());
				},
				image: url + "/amazonapps.btn.png"
		});
		
		ed.addButton('wpAppbox_AppStoreButton', {
			title : '(Mac) App Store Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_appstore_button());
			},
			image: url + "/appstore.btn.png"
		});
		
		ed.addButton('wpAppbox_ChromeWebStoreButton', {
			title : 'Chrome Web Store Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_chromewebstore_button());
			},
			image: url + "/chromewebstore.btn.png"
		});
		
		ed.addButton('wpAppbox_FirefoxAddonButton', {
			title : 'Firefox Erweiterungen Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_firefoxaddon_button());
			},
			image: url + "/firefoxaddon.btn.png"
		});
		
		ed.addButton('wpAppbox_FirefoxMarketplaceButton', {
			title : 'Firefox Marketplace Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_firefoxmarketplace_button());
			},
			image: url + "/firefoxmarketplace.btn.png"
		});
		
		ed.addButton('wpAppbox_GoodOldGamesButton', {
			title : 'Good Old Games (GOG.com) Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_goodoldgames_button());
			},
			image: url + "/goodoldgames.btn.png"
		});
		
		ed.addButton('wpAppbox_GooglePlayButton', {
			title : 'Google Play Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_googleplay_button());
			},
			image: url + "/googleplay.btn.png"
		});
		
		ed.addButton('wpAppbox_OperaAddonsButton', {
			title : 'Opera Add-ons Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_operaaddons_button());
			},
			image: url + "/operaaddons.btn.png"
		});
		
		ed.addButton('wpAppbox_SteamButton', {
			title : 'Steam Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_steam_button());
			},
			image: url + "/steam.btn.png"
		});
		
		ed.addButton('wpAppbox_WindowsStoreButton', {
			title : 'Windows Store Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_windowsstore_button());
			},
			image: url + "/windowsstore.btn.png"
		});
		
		ed.addButton('wpAppbox_WordPressButton', {
			title : 'Wordpress Plugin-Verzeichnis Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_wordpress_button());
			},
			image: url + "/wordpress.btn.png"
		});
		
		ed.addButton('wpAppbox_XDAButton', {
			title : 'XDA Labs (Android) Appbox',
			onclick : function() {
				ed.execCommand('mceInsertContent', false, wpAppbox_wordpress_button());
			},
			image: url + "/xda.btn.png"
		});
	
	}
});
tinymce.PluginManager.add('wpAppboxSingle', tinymce.plugins.wpAppbox_StoreButtons);
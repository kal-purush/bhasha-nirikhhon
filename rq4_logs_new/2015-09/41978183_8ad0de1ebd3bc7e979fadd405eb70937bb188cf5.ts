"use strict";

const regedit		= require("regedit");

import {Promise}	from "bluebird";
import * as _		from "lodash";

/** The configurations for all installed versions of Visual Studio */
export interface VisualStudioConfiguration {
	/** Configuration for Visual Studio 2010 (v10.0), if present */	
	vs2010?: VisualStudioVersion;

	/** Configuration for Visual Studio 2013 (v12.0), if present */	
	vs2013?: VisualStudioVersion;
	
	/** Configuration for Visual Studio 2015 (v14.0), if present */	
	vs2015?: VisualStudioVersion;
}

/** The configuration for a specific version of Visual Studio */
export interface VisualStudioVersion {
	/** The main product install directory */
	installDir: string;
}

export class VisualStudio {
	/** Promise representing cached configuration. */
	private static loader: Promise<VisualStudioConfiguration>; 

	/** 
	 * Load VS information from the registry.
	 * @param refresh {boolean} Refresh the configuration if it has already been loaded? 
	 * @return A promise that resolves to the VisualStudio instance. 
	 */
	public static loadConfiguration(refresh: boolean = false): Promise<VisualStudioConfiguration> {
		if (this.loader && !refresh)
			return this.loader;
		
		this.loader = new Promise<VisualStudioConfiguration>((resolve, reject) => {
			const configurations: VisualStudioConfiguration = {};
			
			// If we're runnin in 64-bit Node then we need to explicitly target the 32-bit registry.
			const wow6432Prefix = process.arch === "x64" ? "\\Wow6432Node" : "";

			const vsKeys = [
				`HKLM\\SOFTWARE${wow6432Prefix}\\Microsoft\\VisualStudio\\10.0\\Setup\\vs`,
				`HKLM\\SOFTWARE${wow6432Prefix}\\Microsoft\\VisualStudio\\12.0\\Setup\\vs`,
				`HKLM\\SOFTWARE${wow6432Prefix}\\Microsoft\\VisualStudio\\14.0\\Setup\\vs`
			];
			regedit.list(vsKeys, (error, items) => {
				if (error)
				{
					reject(error);
					
					return;
				}
	
				if (!items || !items.length)
					resolve(configurations);
				
				let vsInstallDir = this.getInstallDir(
					items[vsKeys[0]]
				);
				if (vsInstallDir)
				{
					configurations.vs2010 = {
						installDir: vsInstallDir
					};
				}

				vsInstallDir = this.getInstallDir(
					items[vsKeys[1]]
				);
				if (vsInstallDir)
				{
					configurations.vs2013 = {
						installDir: vsInstallDir
					};
				}

				vsInstallDir = this.getInstallDir(
					items[vsKeys[2]]
				);
				if (vsInstallDir)
				{
					configurations.vs2015 = {
						installDir: vsInstallDir
					};
				}
				
				resolve(configurations);
			});
		});
		
		return this.loader;
	}	
	
	/** 
	 * Get the product installation directory from the specified Visual Studio registry key.
	 * @param vsKey {} The Visual Studio registry key.
	 * @return The "ProductDir" registry value, or null if not present (or vsKey is null). 
	 */
	private static getInstallDir(vsKey: any): string {
		if (!vsKey)
			return null;
			
		if (vsKey.values.ProductDir && vsKey.values.ProductDir.value)
			return vsKey.values.ProductDir.value;
			
		return null;
	}
}
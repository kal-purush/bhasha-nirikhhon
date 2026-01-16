import { NS } from 'Bitburner';


function getMaxThreads(ns: NS, hostname: string, scriptName: string): number {
    const maxRam = ns.getServerMaxRam(hostname);
    const requiredRam = ns.getScriptRam(scriptName);
    // const usedRam = ns.getServerUsedRam(hostname);

    // const availableRam = maxRam - usedRam;
    // const maxThreads = availableRam / requiredRam;
    const maxThreads = maxRam / requiredRam;
    const finalResult = Math.floor(maxThreads);

    return finalResult;
}


/**
 * Silences all of the annoying logs generated during this script
 */
 function silence(ns) {
	ns.disableLog('disableLog');
	ns.disableLog('scp');
	ns.disableLog('exec');
	ns.disableLog('scan');
	ns.disableLog('killall');
	ns.disableLog('sleep');
	ns.disableLog('getServerMaxRam');
	ns.disableLog('getServerUsedRam');
}

/** @param {NS} ns */
export async function main(ns: NS) {
    silence(ns);

	const localHostname = ns.getHostname();
    const target = ns.args[0] as string;
    const moneyThresh = ns.getServerMaxMoney(target) * 0.9;
	const securityThresh = ns.getServerMinSecurityLevel(target) + 5;
    const requiredHackingLevel = ns.getServerRequiredHackingLevel(target);

    // Main loop:
    while(true) {
        const currentHackingSkill = ns.getPlayer().skills.hacking;
        const currentSecurityLevel = ns.getServerSecurityLevel(target);
        let nextTask: string | boolean = false;
		// Figure out what our next task is:
        if (currentSecurityLevel >= securityThresh) {
            await ns.weaken(target);
        } else if (ns.getServerMoneyAvailable(target as string) < moneyThresh) {
            await ns.grow(target);
        } else if (requiredHackingLevel <= currentHackingSkill) {
            await ns.hack(target);
        }
        await ns.sleep(50);
    }
	
}
import { parse } from "https://deno.land/std@0.119.0/flags/mod.ts";
import { sleep } from "https://deno.land/x/sleep/mod.ts";
import { parse as parseDate, format as formatDate, difference } from "https://deno.land/std@0.95.0/datetime/mod.ts";

var encoder = new TextEncoder();
var decoder = new TextDecoder();

async function heal(parameters){

    const healCommand = `cp -a ${parameters.source} ${parameters.destination} &`
    
    const healProcess = Deno.run({
        cmd: ["bash"],
        stdout: "piped",
        stdin: "piped"
    });

    await healProcess.stdin.write(encoder.encode(healCommand));
    await healProcess.stdin.close();
}

async function check(parameters){

    const integrityProcess = Deno.run({
        cmd: ["bash"],
        stdout: "piped",
        stdin: "piped"
    });

    await integrityProcess.stdin.write(encoder.encode(parameters.integrityCommand));
    await integrityProcess.stdin.close();

    const integrityStatus = await integrityProcess.status()
    const integrityOutput = await integrityProcess.output();

    const displayManagerProcess = Deno.run({
        cmd: ["bash"],
        stdout: "piped",
        stdin: "piped"
    });

    await displayManagerProcess.stdin.write(encoder.encode(parameters.displayCommand));
    await displayManagerProcess.stdin.close();

    const displayManagerStatus = await displayManagerProcess.status();
    const displayManagerOutput = await displayManagerProcess.output();

    integrityProcess.close();
    displayManagerProcess.close();

    if(integrityStatus.success && displayManagerStatus.success){
        
        console.log(`\nPerforming snapshot from ${parameters.source} to ${parameters.destination}`);

        const snapshotProcess = Deno.run({
            cmd: ["btrfs","subvolume","snapshot",parameters.source,parameters.destination],
            stdout: "piped",
            stdin: "piped"
        });

        const snapshotStatus = await snapshotProcess.status();
        const snapshotOutput = await snapshotProcess.output();

        if(snapshotStatus.success){

            console.log("Snapshot complete!");

            await Deno.writeTextFile("/var/log/medici/last.log", formatDate(new Date(), "dd-MM-yyyy@HH:mm")); 

        }else{

            console.log("\nSnapshot in inconsistent state: Exiting...");
            console.log(decoder.decode(snapshotOutput));

            Deno.exit(1)
        }


    }else{
        
        console.log("\nSystem in inconsistent state: Exiting...")
        console.log(decoder.decode(displayManagerOutput));
        console.log(decoder.decode(integrityOutput));

        Deno.exit(1)
    }
}

const parameters = parse(Deno.args, {
  string: ["destination","source","partition",""],
  boolean: ["daily","weekly","monthly"],
  default: { destination: "/mnt/.snapshots", source: "/", partition:"/dev/mmblk0p2", monthly:true }
});

parameters.integrityCommand = `btrfs device stats -c ${parameters.partition}`;
parameters.displayCommand = `systemctl is-failed --quiet lightdm && cat /var/log/lightdm/lightdm.log | egrep --quiet '\sFailed\s|\sexited\s|\sexiting\s' || exit 1`;

if(parameters._[0] == "run"){

    console.log("Running autonomously!");

    try{

        const lastBackup = await Deno.readTextFile("/var/log/medici/last.log")

        var date = parseDate(lastBackup,"dd-MM-yyyy@HH:mm");
        console.log("reading lastLog");


    }catch(_){


        var date = new Date();
        console.log("creating lastLog");

        await Deno.writeTextFile("/var/log/medici/last.log", formatDate(date, "dd-MM-yyyy@HH:mm"));         
    }

    if(parameters.daily){

        var gap = 1;

    }else if(parameters.weekly){

        var gap = 7;

    }else{

        var gap = 30;
    }

    while(true){

        await sleep(60);
        
        check(parameters);

        const now = new Date();
        console.log(difference(now, date, { units: ["days"] }).days >= gap);

        if(difference(now, date, { units: ["days"] }).days >= gap){
            
            heal(parameters);
            
            await Deno.writeTextFile("/var/log/medici/last.log", formatDate(date, "dd-MM-yyyy@HH:mm"));
            date = new Date();
        }
    }
    

}else{

    console.log("Running with CRON!");
}

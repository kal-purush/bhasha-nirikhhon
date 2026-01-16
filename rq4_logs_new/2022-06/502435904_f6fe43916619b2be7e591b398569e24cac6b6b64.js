const inq = require('inquirer');
const fs = require('fs');
const os = require('os')

const forbiddenChars = '\\|/.:*?"\'`<>#';
const rawpath = `${os.userInfo().homedir}\\CustomRegData.json`;
if(!fs.existsSync(rawpath)) fs.writeFileSync(rawpath, '{"selected":"data","lists":{"data":{}}}')
const reg = JSON.parse(fs.readFileSync(rawpath));
function saveReg() {
	fs.writeFileSync(rawpath, JSON.stringify(reg));
}


const listActs = {

	USE(l) {
		reg.selected = l;
		saveReg();
		//main();
	},

	RENAME(l) {
		inq.prompt([{
			name: "0",
			message: "Input the new name for list: "+l+".",
			prefix: `[${reg.selected}-lists-${l}]`
		}]).then(c => {
			let valid = true;
			for (let i = 0; i<c[0].length; i++) {
				for (fci in forbiddenChars) {
					if (c[0][i] == forbiddenChars[fci]) valid = false;
				}
			}

			if (!valid) {
				console.log("Invalid Key Name! Name shouldn't include: "+forbiddenChars);
				//main();
				return;
			}

			if (reg.lists[c[0]] == undefined) {
				reg.lists[c[0]] = reg.lists[l];
				if (l == reg.selected) reg.selected = c[0];
				delete reg.lists[l];
				saveReg();
			} else console.log("List name is already in use or is the same!");

			//main();
		});
	},

	DELETE(l) {
		if (l == reg.selected) console.log("Cannot delete a list that is currently in use!");
		else {
			delete reg.lists[l];
			saveReg();
		}

		//main();
	},

	BACK() {
		//main();
	}

}

const acts = {

	"ECHO KEYS"() {
		let k = Object.keys(reg.lists[reg.selected]);
		let l = "\n";
		let n = 0;
		for (i in k) {
			l += k[i];
			if (i < k.length - 1) l += ", ";
			if (++n % 6 == 0) l += "\n";
		}
		console.log(l);
		//main();
	},

	"EDIT KEY"() {
		inq.prompt([{
			name: "0",
			message: "Input key name.",
			prefix: `[${reg.selected}-edit]`
		}]).then(c => {
			let cd = new Date();
			let dayName = ["sunday","monday","tuesday","wednesday","thursday","friday","saturday"][cd.getDay()];
			let az = (i, m) => {
				if (m == undefined) m = 2;

				if (m>=2 && i < 10) i = "0" + i;
				if (m>=3 && i < 100) i = "0" + i;
				if (m>=4 && i < 1000) i = "0" + i;
				
				return i;
			};

			c[0] = c[0].trim()
				.replace(/@dt:full/g, `${az(cd.getHours())}-${az(cd.getMinutes())}-${az(cd.getSeconds())}:${az(cd.getMonth())}-${az(cd.getDate())}-${cd.getFullYear()}`)
				.replace(/@dt:time/g, `${az(cd.getHours())}-${az(cd.getMinutes())}-${az(cd.getSeconds())}`)
				.replace(/@dt:date/g, `${az(cd.getMonth())}-${az(cd.getDate())}-${cd.getFullYear()}`)
				.replace(/@dt:hours/g, az(cd.getHours()))
				.replace(/@dt:minutes/g, az(cd.getMinutes()))
				.replace(/@dt:seconds/g, az(cd.getSeconds()))
				.replace(/@dt:milliseconds/g, az(cd.getMilliseconds(), 3))
				.replace(/@dt:month/g, az(cd.getMonth()))
				.replace(/@dt:dayname/g, dayName)
				.replace(/@dt:day/g, az(cd.getDate()))
				.replace(/@dt:year/g, az(cd.getFullYear(), 4));
			
			if (c[0].length <= 0 || c[0].length > 30) {
				console.log("Name must be bigger than 0 and less than 31.");
				//main();
				return;
			}

			let valid = true;
			for (let i = 0; i<c[0].length; i++) {
				for (fci in forbiddenChars) {
					if (c[0][i] == forbiddenChars[fci]) valid = false;
				}
			}

			if (!valid) {
				console.log("Invalid Key Name! Name shouldn't include: "+forbiddenChars);
				//main();
			} else {
				let prom = inq.prompt([{
					name: "0",
					message: "View/edit via text editor.",
					prefix: `[${reg.selected}-edit]`,
					type: "editor",
					default: reg.lists[reg.selected][c[0]] !== undefined ? reg.lists[reg.selected][c[0]] :
						  "This is a vacant key value. Replace this text with what you want to input into the key. \n"
						+ "Also, DO NOT SAVE OR DISCARD ON EXIT. Instead, click save or CTRL+S before exiting. \n"
						+ "For some unknown reason, saving and exiting at the same time softlocks the program. \n"
						+ "\n"
						+ "If the program doesn't respond and your PC isn't freezing up (the program cannot halt your PC), \n"
						+ "then just restart the program by closing the CMD prompt and opening the program after it closes."
				}]);
				
				//prom.catch(main);
				prom.then(c2 => {
					if (reg.lists[reg.selected][c[0]] != c2[0]) {
						reg.lists[reg.selected][c[0]] = c2[0];
						saveReg();
					}
					//main();
				});
			}
		});
	},

	"DELETE KEY"() {
		inq.prompt([{
			name: "0",
			message: "Input key name.",
			prefix: `[${reg.selected}-del]`
		}]).then(c => {
			delete reg.lists[reg.selected][c[0]];
			saveReg();
			//main();
		});
	},

	"EDIT LISTS"() {
		inq.prompt([{
			name: "0",
			message: "Select a list, create a new one, or delete one.",
			prefix: `[${reg.selected}-lists]`,
			type: "list",
			choices: Object.keys(reg.lists).concat([new inq.Separator("\ "), ".ADD", ".BACK"])
		}]).then(c => {
			if (c[0] == ".ADD") {
				inq.prompt([{
					name: "0",
					message: "Input new list name.",
					prefix: `[${reg.selected}-lists-new]`
				}]).then(c => {
					c[0] = c[0].trim();

					if (c[0].length <= 0 || c[0].length > 30) {
						console.log("Name must be bigger than 0 and less than 31.");
						//main();
						return;
					}

					let valid = true;
					for (let i = 0; i<c[0].length; i++) {
						for (fci in forbiddenChars) {
							if (c[0][i] == forbiddenChars[fci]) valid = false;
						}
					}

					if (!valid) {
						console.log("Invalid Key Name! Name shouldn't include: "+forbiddenChars);
						//main();
						return;
					}


					if (reg.lists[c[0]] == undefined) {
						reg.lists[c[0]] = {};
						saveReg();
					} else {
						console.log("List already exists!");
					}


					//main();
				});
			} else if (reg.lists[c[0]] != undefined) {
				inq.prompt([{
					name: "0",
					message: "Select an action to commit on "+c[0]+".",
					prefix: `[${reg.selected}-lists-${c[0]}]`,
					type: "list",
					choices: Object.keys(listActs)
				}]).then(c2 => {
					listActs[c2[0]](c[0]);
				});
			} else main();
		});
	}

}


function main() {
	console.log("\n")
	inq.prompt([{
		name: "0",
		message: "Select an action.",
		prefix: `[${reg.selected}]`,
		type: "list",
		choices: Object.keys(acts)
	}]).then(c => {
		console.clear();
		try{acts[c[0]]()}catch(er) {
			console.warn("Unhandled error found: "+er);
			//main();
		}
	});
}

main();
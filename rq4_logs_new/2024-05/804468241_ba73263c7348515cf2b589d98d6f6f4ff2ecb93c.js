document.addEventListener('DOMContentLoaded', () => {
    //these are currently pretty randomly choosen values, as I did not even try some yet.
    //these might be configurable in the future
    //there is some lost potential for synergies between stratagems that should add value
    const stratagems = [
		{ botvalue: 20, bugvalue: 15, heavy : 0, dmgtype : 'single', name: 'HMG Emplacement', type: 'Other', enabled: true, imgSrc: 'assets/Bridge/HMG Emplacement.svg', category: 'bridge' },
		{ botvalue: 54, bugvalue: 58, heavy : 0, dmgtype : 'util', name: 'Orbital EMS Strike', type: 'Bridge', enabled: true, imgSrc: 'assets/Bridge/Orbital EMS Strike.svg', category: 'bridge' },
		{ botvalue: 37, bugvalue: 95, heavy : 0, dmgtype : 'aoe', name: 'Orbital Gas Strike', type: 'Bridge', enabled: true, imgSrc: 'assets/Bridge/Orbital Gas Strike.svg', category: 'bridge' },
		{ botvalue: 80, bugvalue: 52, heavy : 1, dmgtype : 'single', name: 'Orbital Precision Strike', type: 'Bridge', enabled: true, imgSrc: 'assets/Bridge/Orbital Precision Strike.svg', category: 'bridge' },
		{ botvalue: 31, bugvalue: 31, heavy : 0, dmgtype : 'util', name: 'Orbital Smoke Strike', type: 'Bridge', enabled: true, imgSrc: 'assets/Bridge/Orbital Smoke Strike.svg', category: 'bridge' },
		{ botvalue: 5, bugvalue: 5, heavy : 0, dmgtype : 'none', name: 'Shield Generator Relay', type: 'Other', enabled: true, imgSrc: 'assets/Bridge/Shield Generator Relay.svg', category: 'bridge' },
		{ botvalue: 1, bugvalue: 50, heavy : 0, dmgtype : 'aoe', name: 'Tesla Tower', type: 'Bridge', enabled: true, imgSrc: 'assets/Bridge/Tesla Tower.svg', category: 'bridge' },
		{ botvalue: 10, bugvalue: 10, heavy : 0, dmgtype : 'aoe', name: 'Anti-Personnel Minefield', type: 'Engineering Bay', enabled: true, imgSrc: 'assets/Engineering Bay/Anti-Personnel Minefield.svg', category: 'engineering-bay' },
		{ botvalue: 0, bugvalue: 0, heavy : 1, dmgtype : 'aoe', name: 'Anti-Tank Mines', type: 'Engineering Bay', enabled: false, imgSrc: 'assets/Engineering Bay/Anti-Tank Mines.svg', category: 'engineering-bay' },
		{ botvalue: 40, bugvalue: 90, heavy : 0, dmgtype : 'aoe', hasBackpack: 0, name: 'Arc Thrower', type: 'Weapon', enabled: true, imgSrc: 'assets/Engineering Bay/Arc Thrower.svg', category: 'engineering-bay' },
		{ botvalue: 20, bugvalue: 0, heavy : 0, dmgtype : 'none', name: 'Ballistic Shield Backpack', type: 'Backpack', enabled: true, imgSrc: 'assets/Engineering Bay/Ballistic Shield Backpack.svg', category: 'engineering-bay' },
		{ botvalue: 50, bugvalue: 50, heavy : 0, dmgtype : 'aoe', hasBackpack: 0, name: 'Grenade Launcher', type: 'Weapon', enabled: true, imgSrc: 'assets/Engineering Bay/Grenade Launcher.svg', category: 'engineering-bay' },
		{ botvalue: 25, bugvalue: 60, heavy : 0, dmgtype : 'single', name: 'Guard Dog Rover', type: 'Backpack', enabled: true, imgSrc: 'assets/Engineering Bay/Guard Dog Rover.svg', category: 'engineering-bay' },
		{ botvalue: 10, bugvalue: 15, heavy : 0, dmgtype : 'aoe', name: 'Incendiary Mines', type: 'Engineering Bay', enabled: true, imgSrc: 'assets/Engineering Bay/Incendiary Mines.svg', category: 'engineering-bay' },
		{ botvalue: 60, bugvalue: 30, heavy : 0, dmgtype : 'single', hasBackpack: 0, name: 'Laser Cannon', type: 'Weapon', enabled: true, imgSrc: 'assets/Engineering Bay/Laser Cannon.svg', category: 'engineering-bay' },
		{ botvalue: 80, bugvalue: 90, heavy : 1, dmgtype : 'single', hasBackpack: 0, name: 'Quasar Cannon', type: 'Weapon', enabled: true, imgSrc: 'assets/Engineering Bay/Quasar Cannon.svg', category: 'engineering-bay' },
		{ botvalue: 85, bugvalue: 90, heavy : 0, dmgtype : 'none', name: 'Shield Generator Pack', type: 'Backpack', enabled: true, imgSrc: 'assets/Engineering Bay/Shield Generator Pack.svg', category: 'engineering-bay' },
		{ botvalue: 80, bugvalue: 65, heavy : 0, dmgtype : 'none', name: 'Supply Pack', type: 'Backpack', enabled: true, imgSrc: 'assets/Engineering Bay/Supply Pack.svg', category: 'engineering-bay' },
		{ botvalue: 75, bugvalue: 55, heavy : 1, dmgtype : 'single', name: 'Eagle 110MM Rocket Pods', type: 'Hangar', enabled: true, imgSrc: 'assets/Hangar/Eagle 110MM Rocket Pods.svg', category: 'hangar' },
		{ botvalue: 85, bugvalue: 85, heavy : 1, dmgtype : 'single', name: 'Eagle 500KG Bomb', type: 'Hangar', enabled: true, imgSrc: 'assets/Hangar/Eagle 500KG Bomb.svg', category: 'hangar' },
		{ botvalue: 100, bugvalue: 100, heavy : 1, dmgtype : 'aoe', name: 'Eagle Airstrike', type: 'Hangar', enabled: true, imgSrc: 'assets/Hangar/Eagle Airstrike.svg', category: 'hangar' },
		{ botvalue: 50, bugvalue: 70, heavy : 0, dmgtype : 'aoe', name: 'Eagle Cluster Bomb', type: 'Hangar', enabled: true, imgSrc: 'assets/Hangar/Eagle Cluster Bomb.svg', category: 'hangar' },
		{ botvalue: 40, bugvalue: 75, heavy : 0, dmgtype : 'aoe', name: 'Eagle Napalm Airstrike', type: 'Hangar', enabled: true, imgSrc: 'assets/Hangar/Eagle Napalm Airstrike.svg', category: 'hangar' },
		{ botvalue: 20, bugvalue: 15, heavy : 0, dmgtype : 'util', name: 'Eagle Smoke Strike', type: 'Hangar', enabled: true, imgSrc: 'assets/Hangar/Eagle Smoke Strike.svg', category: 'hangar' },
		{ botvalue: 30, bugvalue: 35, heavy : 0, dmgtype : 'aoe', name: 'Eagle Strafing Run', type: 'Hangar', enabled: true, imgSrc: 'assets/Hangar/Eagle Strafing Run.svg', category: 'hangar' },
		{ botvalue: 45, bugvalue: 65, heavy : 0, dmgtype : 'none', name: 'Jump Pack', type: 'Backpack', enabled: true, imgSrc: 'assets/Hangar/Jump Pack.svg', category: 'hangar' },
		{ botvalue: 15, bugvalue: 15, heavy : 0, dmgtype : 'aoe', name: 'Orbital 120MM HE Barrage', type: 'Orbital Cannons', enabled: true, imgSrc: 'assets/Orbital Cannons/Orbital 120MM HE Barrage.svg', category: 'orbital-cannons' },
		{ botvalue: 40, bugvalue: 40, heavy : 1, dmgtype : 'aoe', name: 'Orbital 380MM HE Barrage', type: 'Orbital Cannons', enabled: true, imgSrc: 'assets/Orbital Cannons/Orbital 380MM HE Barrage.svg', category: 'orbital-cannons' },
		{ botvalue: 15, bugvalue: 15, heavy : 0, dmgtype : 'aoe', name: 'Orbital Airburst Strike', type: 'Orbital Cannons', enabled: true, imgSrc: 'assets/Orbital Cannons/Orbital Airburst Strike.svg', category: 'orbital-cannons' },
		{ botvalue: 20, bugvalue: 35, heavy : 0, dmgtype : 'aoe', name: 'Orbital Gatling Barrage', type: 'Orbital Cannons', enabled: true, imgSrc: 'assets/Orbital Cannons/Orbital Gatling Barrage.svg', category: 'orbital-cannons' },
		{ botvalue: 95, bugvalue: 95, heavy : 1, dmgtype : 'single', name: 'Orbital Laser', type: 'Orbital Cannons', enabled: true, imgSrc: 'assets/Orbital Cannons/Orbital Laser.svg', category: 'orbital-cannons' },
		{ botvalue: 85, bugvalue: 95, heavy : 1, dmgtype : 'single', name: 'Orbital Railcannon Strike', type: 'Orbital Cannons', enabled: true, imgSrc: 'assets/Orbital Cannons/Orbital Railcannon Strike.svg', category: 'orbital-cannons' },
		{ botvalue: 30, bugvalue: 15, heavy : 0, dmgtype : 'aoe', name: 'Orbital Walking Barrage', type: 'Orbital Cannons', enabled: true, imgSrc: 'assets/Orbital Cannons/Orbital Walking Barrage.svg', category: 'orbital-cannons' },
		{ botvalue: 25, bugvalue: 30, heavy : 0, dmgtype : 'aoe', hasBackpack: 1, name: 'Airburst Rocket Launcher', type: 'Weapon', enabled: true, imgSrc: 'assets/Patriotic Administration Center/Airburst Rocket Launcher.svg', category: 'administration' },
		{ botvalue: 100, bugvalue: 30, heavy : 0, dmgtype : 'single', hasBackpack: 0, name: 'Anti-Materiel Rifle', type: 'Weapon', enabled: true, imgSrc: 'assets/Patriotic Administration Center/Anti-Materiel Rifle.svg', category: 'administration' },
		{ botvalue: 100, bugvalue: 70, heavy : 0, dmgtype : 'single', hasBackpack: 1, name: 'Autocannon', type: 'Weapon', enabled: true, imgSrc: 'assets/Patriotic Administration Center/Autocannon.svg', category: 'administration' },
		{ botvalue: 60, bugvalue: 95, heavy : 1, dmgtype : 'single', hasBackpack: 0, name: 'Expendable Anti-Tank', type: 'Weapon', enabled: true, imgSrc: 'assets/Patriotic Administration Center/Expendable Anti-Tank.svg', category: 'administration' },
		{ botvalue: 0, bugvalue: 80, heavy : 0, dmgtype : 'aoe', hasBackpack: 0, name: 'Flamethrower', type: 'Weapon', enabled: true, imgSrc: 'assets/Patriotic Administration Center/Flamethrower.svg', category: 'administration' },
		{ botvalue: 25, bugvalue: 15, heavy : 0, dmgtype : 'single', hasBackpack: 0, name: 'Heavy Machine Gun', type: 'Weapon', enabled: true, imgSrc: 'assets/Patriotic Administration Center/Heavy Machine Gun.svg', category: 'administration' },
		{ botvalue: 25, bugvalue: 50, heavy : 0, dmgtype : 'single', hasBackpack: 0, name: 'Machine Gun', type: 'Weapon', enabled: true, imgSrc: 'assets/Patriotic Administration Center/Machine Gun.svg', category: 'administration' },
		{ botvalue: 60, bugvalue: 50, heavy : 0, dmgtype : 'single', hasBackpack: 0, name: 'Railgun', type: 'Weapon', enabled: true, imgSrc: 'assets/Patriotic Administration Center/Railgun.svg', category: 'administration' },
		{ botvalue: 80, bugvalue: 80, heavy : 1, dmgtype : 'single', hasBackpack: 1, name: 'Recoilless Rifle', type: 'Weapon', enabled: true, imgSrc: 'assets/Patriotic Administration Center/Recoilless Rifle.svg', category: 'administration' },
		{ botvalue: 85, bugvalue: 70, heavy : 1, dmgtype : 'single', hasBackpack: 1, name: 'Spear', type: 'Weapon', enabled: true, imgSrc: 'assets/Patriotic Administration Center/Spear.svg', category: 'administration' },
		{ botvalue: 55, bugvalue: 65, heavy : 0, dmgtype : 'single', hasBackpack: 0, name: 'Stalwart', type: 'Weapon', enabled: true, imgSrc: 'assets/Patriotic Administration Center/Stalwart.svg', category: 'administration' },
		{ botvalue: 75, bugvalue: 75, heavy : 1, dmgtype : 'single', name: 'Autocannon Sentry', type: 'Tower', enabled: true, imgSrc: 'assets/Robotics Workshop/Autocannon Sentry.svg', category: 'workshop' },
		{ botvalue: 75, bugvalue: 75, heavy : 0, dmgtype : 'util', name: 'EMS Mortar Sentry', type: 'Tower', enabled: true, imgSrc: 'assets/Robotics Workshop/EMS Mortar Sentry.svg', category: 'workshop' },
		{ botvalue: 30, bugvalue: 80, heavy : 0, dmgtype : 'aoe', name: 'Gatling Sentry', type: 'Tower', enabled: true, imgSrc: 'assets/Robotics Workshop/Gatling Sentry.svg', category: 'workshop' },
		{ botvalue: 55, bugvalue: 35, heavy : 0, dmgtype : 'single', name: 'Guard Dog', type: 'Backpack', enabled: true, imgSrc: 'assets/Robotics Workshop/Guard Dog.svg', category: 'workshop' },
		{ botvalue: 0, bugvalue: 5, heavy : 0, dmgtype : 'single', name: 'Machine Gun Sentry', type: 'Tower', enabled: false, imgSrc: 'assets/Robotics Workshop/Machine Gun Sentry.svg', category: 'workshop' },
		{ botvalue: 80, bugvalue: 50, heavy : 0, dmgtype : 'aoe', name: 'Mortar Sentry', type: 'Tower', enabled: true, imgSrc: 'assets/Robotics Workshop/Mortar Sentry.svg', category: 'workshop' },
		{ botvalue: 60, bugvalue: 30, heavy : 1, dmgtype : 'single', name: 'Patriot Exosuit', type: 'Other', enabled: true, imgSrc: 'assets/Robotics Workshop/Patriot Exosuit.svg', category: 'workshop' },
		{ botvalue: 35, bugvalue: 35, heavy : 1, dmgtype : 'single', name: 'Rocket Sentry', type: 'Tower', enabled: true, imgSrc: 'assets/Robotics Workshop/Rocket Sentry.svg', category: 'workshop' },
    ];
    
    const stratagemList = document.getElementById('stratagems');
    const randomizeButton = document.getElementById('randomizeButton');
    const loadoutDisplay = document.getElementById('loadoutDisplay');
    const categories = ['bridge', 'engineering-bay', 'hangar', 'orbital-cannons', 'workshop', 'administration'];
    
    // Function to render the stratagem list
    function renderStratagemList() {
        categories.forEach(category => {
            const categoryElement = document.getElementById(category).querySelector('ul');
            categoryElement.innerHTML = '';
            stratagems.filter(strat => strat.category === category).forEach((strat, index) => {
                const li = document.createElement('li');
                li.innerHTML = `<label>
                    <input type="checkbox" data-index="${index}" ${strat.enabled ? 'checked' : ''}>
                    <img src="${strat.imgSrc}" alt="${strat.name}"> ${strat.name} (${strat.type})
                </label>`;
                categoryElement.appendChild(li);
            });
        });
    }
    

    // Update stratagem enabled status
    document.querySelectorAll('.stratagem-list ul').forEach(ul => {
        ul.addEventListener('change', (event) => {
            const index = event.target.getAttribute('data-index');
            stratagems[index].enabled = event.target.checked;
        });
    });
    
    // Function to get random loadout
    function getRandomLoadout() {
        
        let tries = 0;

        while (tries < 5000)
        {
            tries++;

            const enabledStratagems = stratagems.filter(s => s.enabled);
            const guaranteedWeapon = document.getElementById('guaranteedWeapon').checked;
            const guaranteedBackpack = document.getElementById('guaranteedBackpack').checked;
            const maxWeapons = parseInt(document.getElementById('maxWeapons').value, 10);
            const maxBackpacks = parseInt(document.getElementById('maxBackpacks').value, 10);
            const fairnessScale = parseInt(document.getElementById('fairnessScale').value, 10);

            let loadout = [];

            let remainingStratagems = enabledStratagems.filter(s => !loadout.includes(s));        
            let weaponCount = loadout.filter(s => s.type === 'Weapon').length;
            if (weaponCount >= maxWeapons){
                remainingStratagems = remainingStratagems.filter(s => s.type !== 'Weapon');
            }
            let backpackCount = loadout.filter(s => s.type === 'Backpack').length;
            if (backpackCount >= maxBackpacks){
                remainingStratagems = remainingStratagems.filter(s => s.type !== 'Backpack');
            }
            
            if (guaranteedWeapon) {
                const weapons = enabledStratagems.filter(s => s.type === 'Weapon');
                if (weapons.length > 0) {
                    const weapon = weapons[Math.floor(Math.random() * weapons.length)];
                    loadout.push(weapon);
                    remainingStratagems = remainingStratagems.filter(s => s.name !== weapon.name);
                }
            }
            if (guaranteedBackpack) {
                const backpacks = enabledStratagems.filter(s => s.type === 'Backpack');
                const weaponBackpack = loadout.filter(s => s.type === 'Weapon' && s.hasBackpack === 1);
                if (backpacks.length > 0 && weaponBackpack.length === 0) {
                    const backpack = backpacks[Math.floor(Math.random() * backpacks.length)];
                    loadout.push(backpack);
                    remainingStratagems = remainingStratagems.filter(s => s.name !== backpack.name);
                }
            }
        
            // Randomly add remaining stratagems to the loadout
            while (loadout.length < 4 && remainingStratagems.length > 0) {
                weaponCount = loadout.filter(s => s.type === 'Weapon').length;
                backpackCount = loadout.filter(s => s.type === 'Backpack').length + loadout.filter(s => s.hasBackpack === 1).length;
                const randomIndex = Math.floor(Math.random() * remainingStratagems.length);
                const strat = remainingStratagems[randomIndex];
                if (strat.type === 'Weapon' && strat.hasBackpack === 1 && backpackCount >= maxBackpacks) {
                    remainingStratagems = remainingStratagems.filter(s => s.name !== strat.name);
                }
                else if (strat.type === 'Weapon' && weaponCount >= maxWeapons) {
                    remainingStratagems = remainingStratagems.filter(s => s.type !== 'Weapon');
                } else if (strat.type === 'Backpack' && backpackCount >= maxBackpacks) {
                    remainingStratagems = remainingStratagems.filter(s => s.type !== 'Backpack');
                } else {
                    loadout.push(strat);
                    remainingStratagems.splice(randomIndex, 1);
                }
            }

            let botsum = 0;
            let bugsum = 0;
            loadout.forEach( s => {
                botsum += s.botvalue;
                bugsum += s.bugvalue;
            })

            //yeah magic numbers! 40 is just choosen as a place where you want to be able to beat almost anything 
            if(fairnessScale > 40 && (loadout.filter(s => s.heavy === 1).length < 1 || loadout.filter(s => s.dmgtype === 'aoe').length < 1)){
                ; //reroll
            }
            else if(document.getElementById('enemyType').value === 'bugs' && bugsum < (4*fairnessScale - 50)){
                ; //reroll
            }
            else if(document.getElementById('enemyType').value === 'bots' && botsum < (4*fairnessScale - 50)){
                ; //reroll
            }
            else{
                return loadout;
            }
        }

        //dont care currently about a smarter algorithm that does not need to reroll
        //a better way would be to favor stratagems that are close to the fainess value and update the goal(fairness) after each choise
        //-> if the first was bad favor better next
        return loadout;
    }
    
    randomizeButton.addEventListener('click', () => {
        const loadout = getRandomLoadout();
        loadoutDisplay.innerHTML = loadout.map(s => `<img src="${s.imgSrc}" alt="${s.name}" title="${s.name}">`).join('');
    });
    
    // Initial render
    renderStratagemList();
    
    // Collapsible sections logic
    document.querySelectorAll('.collapsible').forEach(button => {
        button.addEventListener('click', function() {
            this.classList.toggle('active');
            const content = this.nextElementSibling;
            if (content.style.display === 'block') {
                content.style.display = 'none';
            } else {
                content.style.display = 'block';
            }
        });
    });
});
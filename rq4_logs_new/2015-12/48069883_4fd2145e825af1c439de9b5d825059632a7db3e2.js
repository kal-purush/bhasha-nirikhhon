(function() {
    var instructions = getInstructions();

    var registerA = [0];
    var registerB = [0];
    
    var nextInstruction = 0;
    
    while (nextInstruction >= 0 && nextInstruction < instructions.length) {
        nextInstruction = instructions[nextInstruction](registerA, registerB, nextInstruction);
    }

    console.log(registerB[0]);
    
})();

function getInstructions() {
    var input = [
    'jio a, +19',
    'inc a',
    'tpl a',
    'inc a',
    'tpl a',
    'inc a',
    'tpl a',
    'tpl a',
    'inc a',
    'inc a',
    'tpl a',
    'tpl a',
    'inc a',
    'inc a',
    'tpl a',
    'inc a',
    'inc a',
    'tpl a',
    'jmp +23',
    'tpl a',
    'tpl a',
    'inc a',
    'inc a',
    'tpl a',
    'inc a',
    'inc a',
    'tpl a',
    'inc a',
    'tpl a',
    'inc a',
    'tpl a',
    'inc a',
    'tpl a',
    'inc a',
    'inc a',
    'tpl a',
    'inc a',
    'inc a',
    'tpl a',
    'tpl a',
    'inc a',
    'jio a, +8',
    'inc b',
    'jie a, +4',
    'tpl a',
    'inc a',
    'jmp +2',
    'hlf a',
    'jmp -7'];

    var instructions = [];
        
    for (var i = 0; i < input.length; i++) {
        var match = input[i].match(/hlf ([a|b])/);
        if (match !== null) {
            var registerToHalve = match[1];
            instructions.push(makeHalve(registerToHalve));
            continue;
        }
        match = input[i].match(/tpl ([a|b])/);
        if (match !== null) {
            var registerToTriple = match[1];
            instructions.push(makeTpl(registerToTriple));
            continue;
        }
        match = input[i].match(/inc ([a|b])/);
        if (match !== null) {
            var registerToIncrement = match[1];
            instructions.push(makeInc(registerToIncrement));
            continue;
        }
        match = input[i].match(/jmp ([+|-])(\d+)/);
        if (match !== null) {
            var jumpDistance = parseInt(match[2]) * (match[1] === '-' ? -1 : 1);
            instructions.push(makeJump(jumpDistance));
            continue;
        }
        match = input[i].match(/jie (a|b), ([+|-])(\d+)/);
        if (match !== null) {
            var registerToCheckEven = match[1];
            var jumpDistanceEven = parseInt(match[3]) * (match[2] === '-' ? -1 : 1);
                
            instructions.push(makeJie(registerToCheckEven, jumpDistanceEven));
            continue;
        }
        match = input[i].match(/jio (a|b), ([+|-])(\d+)/);
        if (match !== null) {
            var registerToCheckOne = match[1];
            var jumpDistanceOne = parseInt(match[3]) * (match[2] === '-' ? -1 : 1);
                
            instructions.push(makeJio(registerToCheckOne, jumpDistanceOne));
            continue;
        }
    }
    
    return instructions;
}

function makeHalve(registerToHalve) {
    return function(a, b, position)  {
        if (registerToHalve === 'a') {
            a[0] = Math.floor(a[0] / 2);
        } else {
            b[0] = Math.floor(b[0] / 2);
        }
        
        return position + 1;
    }
}

function makeTpl(registerToTriple) {
    return function(a, b, position) {
        if (registerToTriple === 'a') {
            a[0] = a[0] * 3;
        } else {
            b[0] = b[0] * 3;
        }
        
        return position + 1;
    };
}

function makeInc(registerToIncrement) {
    return function(a, b, position) {
        if (registerToIncrement === 'a') {
            a[0] = a[0] + 1;
        } else {
            b[0] = b[0] + 1;
        }
        
        return position + 1;
    };
}

function makeJump(jumpDistance) {
    return function(a, b, position) {
        return position + jumpDistance;
    }
}

function makeJie(registerToCheck, jumpDistance) {
    return function(a, b, position) {
        var jump = false;
        
        if (registerToCheck === 'a') {
            jump = (a[0] % 2 === 0);
        } else {
            jump = (b[0] % 2 === 0);
        }
        
        return position + (jump ? jumpDistance : 1);
    };
}

function makeJio(registerToCheck, jumpDistance) {
    return function(a, b, position) {
        var jump = false;
        
        if (registerToCheck === 'a') {
            jump = (a[0] === 1);
        } else {
            jump = (b[0] === 1);
        }
        
        return position + (jump ? jumpDistance : 1);
    };   
}
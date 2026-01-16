/// <reference path="InstructionSet.ts"/>
/// <reference path="Utils.ts" />
//The MIPS Instruction Set Architecture

function Oak_gen_MIPS(): InstructionSet
{
    //Formats and Instructions
    var formats: Format[] = [];
    var instructions: Instruction[] = [];

    //R-Type
    formats.push
    (
        new Format
        (
            "R",
            [
                new BitRange("opcode", 26, 6),
                new BitRange("rs", 21, 5, 1),
                new BitRange("rt", 16, 5, 2),
                new BitRange("rd", 11, 5, 0),
                new BitRange("shamt", 6, 5, null, 0),
                new BitRange("funct", 0, 6)
            ],
            ["rd", "rt", "rs"],
            [Parameter.register, Parameter.register, Parameter.register],
            /[a-zA-Z]+\s*(\$[A-Za-z0-9]+)\s*,\s*(\$[A-Za-z0-9]+)\s*,\s*(\$[A-Za-z0-9]+)/,
            "@mnem @arg, @arg, @arg"
        )
    );

    let rType = formats[formats.length - 1];

    instructions.push
    (
        new Instruction
        (
            "ADD",
            rType,
            ["opcode", "funct"],
            [0x0, 0x20],
            function(core)
            {
                core.rd = core.rfBubble.arguments[0];
                return core.rfBubble.rsData + core.rfBubble.rtData;
            },
            function(core)
            {
                return null;
            },
            function(core)
            {
                core.registerFile.write(core.tcBubble.arguments[0], core.tcBubble.aluOut);
                return 0;
            }
        )
    );

    instructions.push
    (
        new Instruction
        (
            "XOR",
            rType,
            ["opcode", "funct"],
            [0x0, 0x26],
            function(core)
            {
                core.rd = core.rfBubble.arguments[0];
                return core.rfBubble.rsData ^ core.rfBubble.rtData;
            },
            function(core)
            {
                return null;
            },
            function(core)
            {
                core.registerFile.write(core.tcBubble.arguments[0], core.tcBubble.aluOut);
                return 0;
            }
        )
    );

    instructions.push(
        new Instruction(
            "SLT",
            rType,
            ["opcode","funct"],
            [0x0,0x2A],
            function(core)
            {
                core.rd = core.rfBubble.arguments[0];
                return (core.rfBubble.rsData < core.rfBubble.rtData)? 1: 0;
            },
            function(core)
            {
                return null;
            },
            function(core)
            {
                core.registerFile.write(core.tcBubble.arguments[0], core.tcBubble.aluOut);
                return 0;
            }
    ));

    //R-Jump Subtype
    formats.push
    (
        new Format
        (
            "RJ",
            [
                new BitRange("opcode", 26, 6),
                new BitRange("rs", 21, 5, 0),
                new BitRange("rt", 16, 5, null, 0),
                new BitRange("rd", 11, 5, null, 0),
                new BitRange("shamt", 6, 5, null, 0),
                new BitRange("funct", 0, 6)
            ],
            ["rs"],
            [Parameter.register],
            /[a-zA-Z]+\s*(\$[A-Za-z0-9]+)/,
            "@mnem @arg"
        )
    );

    let rjSubtype = formats[formats.length - 1];

    instructions.push(new Instruction(
        "JR",
        rjSubtype,
        ["opcode","funct"],
        [0x0, 0x08],
        function(core)
        {
            return null; //Should be handled earlier 
        },
        function(core)
        {
            core.rd = 31;
            return null;
        },
        function(core)
        {
            return null;
        }
    ));

    //R-Constant Subtype
    formats.push
    (
        new Format
        (
            "RC",
            [
                new BitRange("funct", 0, 32)
            ],
            [],
            [],
            /[a-zA-Z]+/,
            "@mnem"
        )
    );

    let rcSubtype = formats[formats.length - 1];

    instructions.push
    (
        new Instruction
        (
            "SYSCALL",
            rcSubtype,
            ["funct"],
            [0xC],
            function(core)
            {
                this.rd = null;
                return null;
            },
            function(core)
            {
                return null;
            },
            function(core)
            {
                core.ecall();
                throw "SYSCALL";
            }
        )
    );

    instructions.push
    (
        new Instruction
        (
            "NOP",
            rcSubtype,
            ["funct"],
            [0],
            function(core)
            {
                this.rd = null;
                return null;
            },
            function(core)
            {
                return null;
            },
            function(core)
            {
                return null;
            }
        )
    );

    //I-Type
    formats.push
    (
        new Format
        (
            "I",
            [
                new BitRange("opcode", 26, 6),
                new BitRange("rs", 21, 5, 1),
                new BitRange("rt", 16, 5, 0),
                new BitRange("imm", 0, 16, 2)
            ],
            ["rt", "rs", "imm"],
            [Parameter.register, Parameter.register, Parameter.immediate],
            /[a-zA-Z]+\s*(\$[A-Za-z0-9]+)\s*,\s*(\$[A-Za-z0-9]+)\s*,\s*(-?[a-zA-Z0-9_]+)/,
            "@mnem @arg, @arg, @arg"
        )
    );


    let iType = formats[formats.length - 1];

    //I-type instructions
    instructions.push
    (
        new Instruction
        (
            "ADDI",
            iType,
            ["opcode"],
            [0x8],
            function(core)
            {
                core.rd = core.rfBubble.arguments[0];
                return core.rfBubble.rsData + core.rfBubble.arguments[2];
            },
            function(core)
            {
                return null;
            },
            function(core)
            {
                core.registerFile.write(core.tcBubble.arguments[0], core.tcBubble.aluOut);
                return 0;
            }
        )

    );

    //I-Branch Subtype
    formats.push
    (
        new Format
        (
            "IB",
            [
                new BitRange("opcode", 26, 6),
                new BitRange("rs", 21, 5, 0),
                new BitRange("rt", 16, 5, 1),
                new BitRange("imm", 0, 16, 2)
            ],
            ["rt", "rs", "imm"],
            [Parameter.register, Parameter.register, Parameter.special],
            /[a-zA-Z]+\s*(\$[A-Za-z0-9]+)\s*,\s*(\$[A-Za-z0-9]+)\s*,\s*(-?[a-zA-Z0-9_]+)/,
            "@mnem @arg, @arg, @arg",
            0,
            1,
            function(address: number, text: string, bits: number, labels: string[], addresses: number[])
            {
                let array = text.split(""); //Character View
                var result =
                {
                    errorMessage: null,
                    value: null
                };

                var int = NaN;
                let labelLocation = labels.indexOf(text);
                if (labelLocation !== -1)
                {
                    int = addresses[labelLocation];
                }
                else
                {
                    var radix = 10 >>> 0;
                    var splice = false;
                    
                    if (array[0] === "0")
                    {
                        if (array[1] == "b")
                        {
                            radix = 2;
                            splice = true;
                        }
                        if (array[1] == "o")
                        {
                            radix = 8;
                            splice = true;
                        }
                        if (array[1] == "d")
                        {
                            radix = 10;
                            splice = true;
                        }
                        if (array[1] == "x")
                        {
                            radix = 16;
                            splice = true;
                        }
                    }

                    var interpretable = text;
                    if (splice)
                    {
                        interpretable = array.splice(2, array.length - 2).join("");
                    }
                    int = parseInt(interpretable, radix);
                }
                    
                if (isNaN(int))
                {     
                    result.errorMessage = "Offset '" + text + "' is not a recognized label or literal.";
                    return result;
                }

                if ((int & 3) != 0)
                {
                    result.errorMessage = "Branches must be word-aligned.";
                    return result;
                }
                
                int -= address;

                int >>= 2;

                if (rangeCheck(int, 16))
                {
                    result.value = int;
                    return result;
                }
                result.errorMessage = "The value of '" + text + "' is out of range.";
                return result;
            },
            function(value: number, address: number)
            {
                return value << 2;
            }
        )
    );

    let ibSubtype = formats[formats.length - 1];

    instructions.push
    (
        new Instruction
        (
            "BEQ",
            ibSubtype,
            ["opcode"],
            [0x04],
            function(core)
            {
                return (core.rfBubble.rsData == core.rfBubble.rtData)? 1: 0;
            },
            function(core)
            {
                return null;
            },
            function(core)
            {
                return null;
            }
        )
    );

    instructions.push
    (
        new Instruction
        (
            "BNE",
            ibSubtype,
            ["opcode"],
            [0x05],
            function(core)
            {
                return (core.rfBubble.rsData != core.rfBubble.rtData)? 1: 0;
            },
            function(core)
            {
                return null;
            },
            function(core)
            {
                return null;
            }
        )

    );

    instructions.push
    (
        new Instruction
        (
            "BLE",
            ibSubtype,
            ["opcode"],
            [0x06],
            function(core)
            {
                return (core.rfBubble.rsData <= core.rfBubble.rtData)? 1: 0;
            },
            function(core)
            {
                return null;
            },
            function(core)
            {
                return null;
            }
        )

    );
    
    //I Load/Store Subtype
    formats.push
    (
        new Format
        (
            "ILS",
            [
                new BitRange("opcode", 26, 6),
                new BitRange("rs", 21, 5, 2),
                new BitRange("rt", 16, 5, 0),
                new BitRange("imm", 0, 16, 1)
            ],
            ["rt", "imm", "rs"],
            [Parameter.register, Parameter.immediate, Parameter.register],
            /[a-zA-Z]+\s*(\$[A-Za-z0-9]+)\s*,\s*(-?0?[boxd]?[0-9A-F]+)\s*\(\s*(\$[A-Za-z0-9]+)\s*\)/,
            "@mnem @arg, @arg(@arg)"
        )
    );

    let ilsSubtype = formats[formats.length - 1];


    //TO-DO: Verify function(core) functionality
    
    instructions.push
    (
        new Instruction
        (
            "LW",
            ilsSubtype,
            ["opcode"],
            [0x23],
            function(core)
            {
                core.rd = null;
                return core.rfBubble.rsData + core.rfBubble.arguments[1];
            },
            function(core)
            {
                var bytes = core.memcpy(core.df1Bubble.aluOut, 4)
                if (bytes == null)
                {
                    return -1;
                }
                core.df2Bubble.readData = catBytes(bytes);
                console.log("read data" + core.df2Bubble.readData);
                return 0;
            },
            function(core)
            {
                core.registerFile.write(arguments[0], core.tcBubble.readData);
                return 0;
            }
        )
    );

    instructions.push
    (
        new Instruction
        (
            "SW",
            ilsSubtype,
            ["opcode"],
            [0x2B],
            function(core)
            {
                core.rd = arguments[0];
                return core.rfBubble.rsData + core.rfBubble.arguments[1];
            },
            function(core)
            {
                var bytes = [];
                var value = core.eBubble.rtData;
                console.log("rtData @ sw " + value);
                bytes.push(value & 255);
                value = value >>> 8;
                bytes.push(value & 255);
                value = value >>> 8;
                bytes.push(value & 255);
                value = value >>> 8;
                bytes.push(value & 255);
                console.log(core.df1Bubble.aluOut, bytes);
                if (core.memset(core.df1Bubble.aluOut, bytes))
                {
                    return 0;
                }
                return -1;
                 
            },
            function(core)
            {
                return null;
            }
        )
    );    

    //J-Type
    formats.push
    (
        new Format
        (
            "J",
            [
                new BitRange("opcode", 26, 6),
                new BitRange("imm", 0, 26, 0, null, 32)
            ],
            ["imm"],
            [Parameter.special],
            /[A-z]+\s*([A-Za-z0-9_]+)/,
            "@mnem @arg",
            0,
            0,
            function(address: number, text: string, bits: number, labels: string[], addresses: number[])
            {
                let array = text.split(""); //Character View
                var result =
                {
                    errorMessage: null,
                    value: null
                };

                var int = NaN;
                let labelLocation = labels.indexOf(text);
                if (labelLocation !== -1)
                {
                    int = addresses[labelLocation];
                }
                else
                {
                    var radix = 10 >>> 0;
                    var splice = false;
                    
                    if (array[0] === "0")
                    {
                        if (array[1] == "b")
                        {
                            radix = 2;
                            splice = true;
                        }
                        if (array[1] == "o")
                        {
                            radix = 8;
                            splice = true;
                        }
                        if (array[1] == "d")
                        {
                            radix = 10;
                            splice = true;
                        }
                        if (array[1] == "x")
                        {
                            radix = 16;
                            splice = true;
                        }
                    }

                    var interpretable = text;
                    if (splice)
                    {
                        interpretable = array.splice(2, array.length - 2).join("");
                    }
                    int = parseInt(interpretable, radix);
                }
                    
                if (isNaN(int))
                {     
                    result.errorMessage = "Offset '" + text + "' is not a recognized label or literal.";
                    return result;
                }

                if ((int >>> 28) == (address >>> 28))
                {
                    if ((int & 3 ) == 0)
                    {
                        result.value = (int & 0x0ffffffc) >>> 2;
                        return result;
                    }
                    result.errorMessage = "Jumps must be word-aligned.";
                    return result;
                }
                result.errorMessage = "The value of '" + text + "' is out of range.";
                return result;
            },
            function(value: number, address: number)
            {
                return (value << 2) | (address & 0xf0000000);
            }
        )
    );

    let jType = formats[formats.length - 1];

    instructions.push(new Instruction(
        "J",
        jType,
        ["opcode"],
        [0x2],
        function(core) {
            return null;
        },
        function(core) {
            return null;
        },
        function(core) {
            return null;
        }
    ));

    instructions.push(new Instruction(
        "JAL",
        jType,
        ["opcode"],
        [0x3],
        function(core) {
            return null;
        },
        function(core) {
            return null;
        },
        function(core) {
            core.registerFile.write(31, core.tcBubble.pc);
            return 0;
        }
    ));

    instructions.push(new Instruction(
        "JUMP_PROCEDURE",
        jType,
        ["opcode"],
        [0x63],
        function(core) {
            return null;
        },
        function(core) {
            return null;
        },
        function(core) {
            return null;
        }
    ));

    instructions.push(new Instruction(
        "RETURN_PROCEDURE",
        jType,
        ["opcode"],
        [0x64],
        function(core) {
            return null;
        },
        function(core) {
            return null;
        },
        function(core) {
            return null;
        }
    ));

    /*
        ARGUMENT PROCESSOR
        Does what it says on the tin. It needs quite a bit of information, but otherwise successfully interprets
        any MIPS argument.
    */
    let process = function(address: number, text: string, type: Parameter, bits: number, labels: string[], addresses: number[])
    {
        let array = text.split(""); //Character View
        var result =
        {
            errorMessage: null,
            value: null
        };
        switch(type)
        {
        case Parameter.register:
                var registerNo: number;
                let index = this.abiNames.indexOf(text);
                if (index !== -1)
                {
                    result.value = index;
                    return result;
                }
                if (array[0] !== "$")
                {
                    result.errorMessage = "Register " + text + " does not exist.";
                    return result;
                }
                registerNo = parseInt(array.splice(1, array.length - 1).join(""));
                if (0 <= registerNo && registerNo <= 31)
                {
                    result.value = registerNo;
                    return result;
                }
                else
                {
                    result.errorMessage = "Register " + text + " does not exist.";
                    return result;
                }


        case Parameter.immediate:
            //Label
            var int = NaN;
            let labelIndex = labels.indexOf(text);
            if (labelIndex !== -1)
            {
                int = addresses[labelIndex];
            }
            else if (array.length === 3 && (array[0] == "\'") && (array[2] == "\'"))
            {
                int = array[1].charCodeAt(0);
            }
            else
            {
                var radix = 10 >>> 0;
                var splice = false;

                if (array[0] === "0")
                {
                    if (array[1] == "b")
                    {
                        radix = 2;
                        splice = true;
                    }
                    if (array[1] == "o")
                    {
                        radix = 8;
                        splice = true;
                    }
                    if (array[1] == "d")
                    {
                        radix = 10;
                        splice = true;
                    }
                    if (array[1] == "x")
                    {
                        radix = 16;
                        splice = true;
                    }
                }

                var interpretable = text;
                if (splice)
                {
                    interpretable = array.splice(2, array.length - 2).join("");
                }

                int = parseInt(interpretable, radix);
            }

            if (isNaN(int))
            {
                result.errorMessage = "Immediate '" + text + "' is not a recognized label, literal or character.";
                return result;
            }

            if (rangeCheck(int, bits))
            {
                result.value = int;
                return result;
            }
            result.errorMessage = "The value of '" + text + "' is out of range.";
            return result;


        case Parameter.offset:
            var int = NaN;
            let labelLocation = labels.indexOf(text);
            if (labelLocation !== -1)
            {
                int = addresses[labelLocation] - address;
            }
            else
            {
                var radix = 10 >>> 0;
                var splice = false;

                if (array[0] === "0")
                {
                    if (array[1] == "b")
                    {
                        radix = 2;
                        splice = true;
                    }
                    if (array[1] == "o")
                    {
                        radix = 8;
                        splice = true;
                    }
                    if (array[1] == "d")
                    {
                        radix = 10;
                        splice = true;
                    }
                    if (array[1] == "x")
                    {
                        radix = 16;
                        splice = true;
                    }
                }

                var interpretable = text;
                if (splice)
                {
                    interpretable = array.splice(2, array.length - 2).join("");
                }

                int = parseInt(interpretable, radix);
            }

            if (isNaN(int))
            {
                result.errorMessage = "Offset '" + text + "' is not a recognized label or literal.";
                return result;
            }

            if (rangeCheck(int, bits))
            {
                result.value = int;
                return result;
            }
            result.errorMessage = "The value of '" + text + "' is out of range.";
            return result;

        default:
            return result;
        }
    }

    /*
        TOKENIZER

        This is the assembler's "first pass" -it does
        primtive lexical analysis and creates an
        address table.
    */
    let tokenize = function(file: string)
    {
        var result =
        {
            errorMessage: null,
            labels: [],
            addresses: [],
            lines: [],
            pc: [],
        };

        var address = 0;
        var text = true;
        var lines = file.split("\n");

        for (var i = 0; i < lines.length; i++)
        {  
            
            var labelExtractor = /\s*(([A-Za-z_][A-Za-z0-9_]*):)?(.*)?/.exec(lines[i]);
            if (labelExtractor == null)
            {
                console.log("Congratulations, you broke regular expressions.")
            }
            if (typeof labelExtractor[2] !== 'undefined')
            {
                result.labels.push(labelExtractor[2]);
                result.addresses.push(address);
            }
            lines[i] = labelExtractor[3];
            if (lines[i] == undefined)
            {
                continue;
            }
            var chars = lines[i].split("");


            //Check for unterminated string/char (also comments)
            var inString = false;
            var commentOut = false;

            //Comments
            for (var j = 0; j < chars.length; j++)
            {
                if (!commentOut)
                {
                    if (chars[j] == "\"" || chars[j] == "\'")
                    {
                        inString = !inString;
                    }
                    else if (inString)
                    {                     
                        if (chars[j] == "\\")
                        {
                            j++; //Escape next character
                        }
                        else if (chars[j] == "\n")
                        {
                            result.errorMessage = "Line " + i + ": Unterminated string.";
                            return result;
                        }
                    }
                    else
                    {
                        if (chars[j] == "#")
                        {
                            commentOut = true;
                            chars.splice(j, 1);
                            j--;
                        }
                    }
                }
                else
                {
                    if (chars[j] !== "\n")
                    {
                        chars.splice(j, 1);
                        j--;
                    }
                    else
                    {
                        commentOut = false;
                    }
                }
            }

            lines[i] = chars.join("");
            
            lines[i] = lines[i].split("' '").join("32");
            
            //These are fine for most purposes, but string directives MUST NOT USE THE ARRAY DIRECTIVES BY ANY MEANS.
            let directives = lines[i].split(" ").filter(function(value: string){ return value.length > 0 });
            
            //Check if whitespace
            if (directives.length === 0)
            {
                continue;
            }

            var directiveChars = directives[0].split("");                

            //Calculate size in bytes
            if (text)
            {
                if (directives[0] === ".data")
                {
                    text = false;
                    if (directives[1] !== undefined)
                    {
                        result.errorMessage = "Line " + i + ": " + directives[1] + " is extraneous. .data does not take any arguments.";
                        return result;
                    }
                }
                else if (directives[0] === ".text")
                {
                    //Do nothing.
                }
                else if (directiveChars[0] === ".")
                {                        
                    result.errorMessage = "Line " + i + ": " + directives[0] + " cannot be in the text section. Aborting.";
                    return result;
                }
                else 
                {
                    address += 4;
                    let instructionIndex = this.mnemonicSearch(directives[0].toUpperCase());
                    if (instructionIndex === -1)
                    {     
                        result.errorMessage = "Line " + i + ": Instruction " + directives[0] + " not found.";
                        return result;
                    }
                }                    
            }
            else
            {
                if (directives[0] == ".text")
                {
                    text = true;
                    if (directives[1] !== undefined)
                    {
                        result.errorMessage = "Line " + i + ": " + directives[1] + " is extraneous. .text does not take any arguments.";
                        return result;
                    }
                }

                else if (directives[0] === ".data")
                {
                    //Do nothing.
                }
                else if (this.dataDirectives.indexOf(directives[0]) !== -1)
                {
                    let index = this.dataDirectives.indexOf(directives[0]);
                    if (this.dataDirectiveSizes[index] !== 0)
                    {
                        let array = directives.join(" ").split(directives[i]).join("").split(",");
                        address += array.length * this.dataDirectiveSizes[index];
                    }
                    else
                    {
                        switch (directives[0])
                        {   
                            case ".asciiz":
                            case ".ascii":
                                var match = /.([A-Za-z]+?)\s*\"(.*)\"\s*(#.*)?$/.exec(lines[i]);
                                if (match == null)
                                {
                                    result.errorMessage = "Line " + i + ": Malformed string directive.";
                                    return result;
                                }
                                let array = match[1].split("");
                                for (var j = 0; j < array.length; j++)
                                {
                                    if (array[j] == "\\")
                                    {
                                        j++;
                                    }
                                    address += 1;
                                }
                            if (directives[0] == ".asciiz")
                            {
                                address += 1;
                            }
                        }
                    }
                }
                else if (directiveChars[0] === ".")
                {
                    result.errorMessage = "Line " + i + ": Unsupported directive " + directives[0] + ".";
                    return result;
                }
                else
                {
                    result.errorMessage = "Line " + i + ": Unrecognized keyword " + directives[0] + ".";
                    return result;
                }
            }
            result.pc.push(address);
        }
        result.lines = lines;
        return result;
    };

     /*
        ASSEMBLER
        This is the fun part.
    */
    let assemble = function(nester: number = null, address: number, lines: string[], labels: string[], addresses: number[])
    {
        var result =
        {
            errorMessage: null,
            machineCode: [],
            size: 0
        };

        var text = true;

        for (var i = 0; i < lines.length; i++)
        {            
            if (typeof lines[i] == 'undefined')
            {
                continue;
            }      
            let directives = lines[i].split(" ").filter(function(value: string){ return value.length > 0 });
            
            //Check if whitespace
            if (directives.length === 0)
            {
                continue;
            }
            
            if (text)
            {
                if (directives[0] === ".data")
                {
                    text = false;
                }
                else if (directives[0] === ".text")
                {
                    //\_(ツ)_/
                }
                else 
                {
                    address += 4;
                    let instructionIndex = this.mnemonicSearch(directives[0].toUpperCase());

                    if (instructionIndex === -1)
                    {
                        result.errorMessage = "Line " + ((nester == null)? "": (nester + ":")) + i + ": Instruction " + directives[0] + " not found.";
                        return result;
                    }
                    let instruction = this.instructions[instructionIndex];
                    let format = instruction.format;
                    let bitRanges = format.ranges;
                    let regex = format.regex;
                    let params = format.parameters;
                    let paramTypes = format.parameterTypes;                        
                    var machineCode = instruction.template();

                    var match = regex.exec(lines[i]);
                    if (match == null)
                    {
                        result.errorMessage = "Line " + ((nester == null)? "": (nester + ":")) + i + ": Argument format for " + directives[0] + " violated.";
                        return result;
                    }
                    var args = match.splice(1, params.length);  

                    for (var j = 0; j < bitRanges.length; j++)
                    {                                             
                        if (bitRanges[j].parameter != null)
                        {
                            var startBit = 0;
                            var endBit: number = null;
                            var bits = bitRanges[j].bits;
                            var field = bitRanges[j].field;

                            var limits = /([A-za-z]+)\s*\[\s*(\d+)\s*:\s*(\d+)\s*\]/.exec(bitRanges[j].field);

                            if (limits != null)
                            {
                                field = limits[1];
                                bits = bitRanges[j].limitlessBits;
                            }

                            let index = format.fieldParameterIndex(field);

                            var register = 0;
                            
                            if(paramTypes[index] !== Parameter.special)
                            {
                                let processed = this.processParameter(address, args[bitRanges[j].parameter], paramTypes[index], bits, labels, addresses);
                                if (processed.errorMessage !== null)
                                {
                                    result.errorMessage = "Line " + ((nester == null)? "": (nester + ":")) + i + ": " + processed.errorMessage;
                                    return result;                            
                                }
                                register = processed.value;
                            }
                            else
                            {
                                let processed = instruction.format.processSpecialParameter(address, args[index], bits, labels, addresses);
                                if (processed.errorMessage !== null)
                                {
                                    result.errorMessage = "Line " + ((nester == null)? "": (nester + ":")) + i + ": " + processed.errorMessage;
                                    return result;                            
                                }
                                register = processed.value;
                            }

                            if (limits != null)
                            {
                                startBit = parseInt(limits[3]);
                                endBit = parseInt(limits[2]);

                                register = register >>> startBit;
                                register = register & ((1 << (endBit - startBit + 1)) - 1);
                            }
                            machineCode = machineCode | ((register & ((1 << bitRanges[j].bits) - 1)) << bitRanges[j].start);  

                        }
                    }

                    for (var j = 0; j < 4; j++)
                    {
                        result.machineCode.push(machineCode & 255);
                        machineCode = machineCode >>> 8;
                    }
                }
            }
            else
            {
                if (directives[0] == ".text")
                {
                    text = true;
                }
                else if (this.dataDirectives.indexOf(directives[0]) !== -1)
                {
                    let index = this.dataDirectives.indexOf(directives[0]);
                    
                    if (this.dataDirectiveSizes[index] !== 0)
                    {
                        let size = this.dataDirectiveSizes[index];
                        let array = lines[i].split("' '").join("'$OAK_SPACE_TEMP'").split(directives[0]).join("").split(" ").join("").split("'$OAK_SPACE_TEMP'").join("' '").split(",");
                        for (var j = 0; j < array.length; j++)
                        {
                            var processed = this.processParameter(address, array[j], Parameter.immediate, size * 8, labels, addresses);
                            if (processed.errorMessage !== null)
                            {
                                result.errorMessage = "Line " + ((nester == null)? "": (nester + ":")) + i + ": " + processed.errorMessage;
                                return result;                            
                            }
                            for (var k = 0; k < size; k++)
                            {
                                address += 1;
                                result.machineCode.push(processed.value & 255);
                                processed.value = processed.value >>> 8;
                            }
                        }
                    }
                    else
                    {
                        switch (directives[0])
                        {   
                            case ".asciiz":
                            case ".ascii":
                            var stringMatch = /.([A-Za-z]+?)\s*\"(.*)\"\s*(#.*)?$/.exec(lines[i]);
                            if (stringMatch == null)
                            {
                                result.errorMessage = "Line " + i + ": Malformed string directive.";
                                return result;
                            }
                            if (stringMatch[1] == undefined)
                            {
                                stringMatch[1] = "";
                            }
                            let characters = stringMatch[1].split("");
                            for (var j = 0; j < characters.length; j++)
                            {
                                if (characters[j] == "\\")
                                {
                                    j++;
                                    if (j + 1 < characters.length)
                                    {
                                        switch (characters[j + 1])
                                        {
                                            case 'n':
                                                result.machineCode.push(10 >>> 0);
                                                break;
                                            case '0':
                                                result.machineCode.push(0 >>> 0);
                                                break;
                                            case "'":
                                                result.machineCode.push(39 >>> 0);
                                                break;
                                            case "\\":
                                                result.machineCode.push(92 >>> 0);
                                                break;
                                            default:
                                                result.machineCode.push(characters[j].charCodeAt(0))                                                             
                                        }
                                    }
                                }
                                else
                                {
                                    result.machineCode.push(characters[j].charCodeAt(0));
                                }
                                
                                address += 1;
                            }
                            if (directives[0] == ".asciiz")
                            {
                                result.machineCode.push(0 >>> 0);
                                address += 1;
                            }
                        }
                    }
                }
            }
        }
        result.size = address;
        return result;
    };


    let abiNames = ["$zero", "$at", "$v0", "$v1", "$a0", "$a1", "$a2", "$a3", "$t0", "$t1", "$t2", "$t3", "$t4", "$t5", "$t6", "$t7", "$s0", "$s1", "$s2", "$s3", "$s4", "$s5", "$s6", "$s7", "$t8", "$t9", "$k0", "$k1", "$gp", "$sp", "$fp", "$ra"];

    return new InstructionSet("mips", 32, formats, instructions, [".word", ".half", ".byte", ".asciiz"], [4, 2, 1, 0], abiNames, process, tokenize, assemble);
}
let MIPS = Oak_gen_MIPS();

class MIPSRegisterFile
{
    private memorySize: number;
    physicalFile: number[];
    abiNames: string[];
    modifiedRegisters: boolean[];

    print()
    {
        console.log("Registers\n------");
        for (var i = 0; i < 32; i++)
        {
            console.log("$" + i.toString(), this.abiNames[i], this.physicalFile[i].toString(), (this.physicalFile[i] >>> 0).toString(16).toUpperCase());
        }
        console.log("------");
    }
    
    read(registerNumber: number)
    {
        if (registerNumber === 0)
        {
            return 0;
        }
        else
        {
            return this.physicalFile[registerNumber];
        }
    }

    write(registerNumber: number, value: number)
    {
        this.physicalFile[registerNumber] = value;
        this.modifiedRegisters[registerNumber] = true;
    }

    getRegisterCount():number
    {
        return 32;
    }


    getModifiedRegisters():boolean[]
    {
        var modReg = this.modifiedRegisters.slice();
        for (var i = 0; i < this.getRegisterCount(); i++) {
            this.modifiedRegisters[i] = false;
        }
        return modReg;
    }

    reset()
    {
        for (var i = 0; i < 32; i++)
        {
            this.physicalFile[i] = 0;
            this.modifiedRegisters[i] = false;
        }
        this.physicalFile[29] = this.memorySize;

    }

    constructor(memorySize: number, abiNames: string[])
    {
        this.physicalFile = [];
        this.modifiedRegisters = [];
        for (var i = 0; i < 32; i++)
        {
            this.physicalFile.push(0);
            this.modifiedRegisters.push(false);
        }
        this.memorySize = memorySize;
        this.physicalFile[29] = memorySize; //stack pointer
        this.abiNames = abiNames;
    }
};

class BranchPredictor
{
    state: number;
    
    sendResult(branched: boolean)
    {
        if (branched)
        {
            this.state -= 1;
            if (this.state < 0)
            {
                this.state = 0;
            }
        }
        else
        {
            this.state += 1;
            if (this.state > 3)
            {
                this.state = 3;
            }
        }
    }

    getPrediction(): boolean
    {
        return (((this.state >> 1) & 1) != 0b1);
    }

    constructor()
    {
        this.state = 0 >>> 0;
    }
}

class MIPSCore //: Core
{
    //Permanent
    instructionSet: InstructionSet;
    memorySize: number;

    //Transient
    cycleCounter: number;
    pc: number;
    pcNext: number;
    memory: number[];
    predictor: BranchPredictor;
    registerFile: MIPSRegisterFile;
    rd: number;

    stackPointer: number;
    stack = [0, 0, 0, 0];

    //Environment Call Lambda (i.e. quit the fucken program)
    ecall: () => void;
    addCycle: (instruction: string, cycleNames: string[]) => void;
    clearPipeline: () => void;

    //Instruction Callback
    instructionCallback: (data: string) => void;


    //Returns bytes on success, null on failure
    memcpy(address: number, bytes: number): number[]
    {
        if (address + bytes > this.memorySize)
        {
            return null;
        }
        
        var result: number[] = [];
        for (var i = 0; i < bytes; i++)
        {
            result.push(this.memory[address + i]);
        }
        return result;
    }

    //Returns boolean indicating success
    //Use to store machine code in memory so it can be executed.
    memset(address: number, bytes: number[]): boolean
    {
        if (address < 0)
        {
            return false;
        }

        if (address + bytes.length > this.memorySize)
        {
            return false;
        }

        for (var i = 0; i < bytes.length; i++)
        {
            this.memory[address + i] = bytes[i];
        }
        return true;
    }

    fetchAndDecode(): any
    {
        var result =
        {
            fetched: null,
            text: null,
            instruction: null,
            arguments: null,
            error: null
        };
        
        if (this.pc < 0)
        {
            result.error = "Fetch Error: Negative program counter.";
            return result;
        }
        let arr = this.memcpy(this.pc, 4);
        if (arr == null)
        {
            result.error = "Fetch Error: Illegal memory access.";
            return result;

        }
        this.pcNext = this.pc + 4;

        var fetched = catBytes(arr);

        let insts = this.instructionSet.instructions;
        var decoded = null;
        var args = [];
        for (var i = 0; i < insts.length; i++)
        {
            if (insts[i].match(fetched))
            {
                decoded = insts[i];
                break;
            }
        }
        if (decoded == null)
        {
            result.error = "Address 0x" + (this.pc - 4).toString(16).toUpperCase() + ": Instruction unrecognized or unsupported.";
            return result;
        }

        let format = decoded.format;
        let bitRanges = format.ranges;
        let params = format.parameters;
        let paramTypes = format.parameterTypes;

        for (var i = 0; i < bitRanges.length; i++)
        {
            if (bitRanges[i].parameter != null)
            {
                var limit = 0;
                var field = bitRanges[i].field;

                var limits = /([A-za-z]+)\s*\[\s*(\d+)\s*:\s*(\d+)\s*\]/.exec(bitRanges[i].field);

                if (limits != null)
                {
                    field = limits[1];
                    limit = parseInt(limits[3]) >>> 0;
                }

                let index = format.fieldParameterIndex(field);
                var bits = bitRanges[i].bits;

                var value = ((fetched >>> bitRanges[i].start) & ((1 << bitRanges[i].bits) - 1)) << limit;
                
                if(paramTypes[index] === Parameter.special)
                {
                    value = decoded.format.decodeSpecialParameter(value, this.pcNext); //Unmangle...
                }

                args[bitRanges[i].parameter] = args[bitRanges[i].parameter] | value;
            }
        }

        for (var i = 0; i < params.length; i++)
        {
            let rangeIndex = format.parameterBitRangeIndex(params[i]);
            if (rangeIndex === -1)
            {
                console.log("Internal error: No field found for parameter " + params[i] + ".");
            }

            var bits = bitRanges[rangeIndex].bits;
            if (bitRanges[rangeIndex].limitlessBits != null)
            {
                bits = bitRanges[rangeIndex].limitlessBits;
            }

            if (decoded.signed && paramTypes[i] != Parameter.register)
            {
                args[i] = signExt(args[i], bits);
            }
        }

        //Jumping in Fetch is probably the easiest way to handle it
        if (decoded.format.name == "J")
        {
            if (decoded.mnemonic == "JUMP_PROCEDURE")
            {
                if (this.stackPointer >= 3)
                {
                    return "Jump procedure stack overflow."
                }
                this.stackPointer += 1;
                this.stack[this.stackPointer] = this.pcNext;
            }
            if (decoded.mnemonic == "RETURN_PROCEDURE")
            {
                if (this.stackPointer < 0)
                {
                    return "Return procedure stack underflow."
                }
                this.stackPointer -= 1;
            }
            this.pcNext = args[0];
        }
        
        //Branching here

        result =
        {
            fetched: fetched,
            text: format.disassemble(decoded.mnemonic, args, this.instructionSet.abiNames),
            instruction: decoded,
            arguments: args,
            error: null
        }
        
        return result;
    }

    //Inter-stage buffers
    ifBubble =
    {
        pc: null,
        fetched: null,
        text: null,
        instruction: null,
        arguments: null,
        valid: false
    };

    isBubble =
    {
        pc: null,
        fetched: null,
        text: null,
        instruction: null,
        arguments: null,
        valid: false
    };

    rfBubble = 
    {
        pc: null,
        fetched: null,
        text: null,
        instruction: null,
        arguments: null,
        valid: false,
        rsData: null,
        rtData: null
    };

    eBubble =
    {
        pc: null,
        fetched: null,
        text: null,
        instruction: null,
        arguments: null,
        valid: false,
        rsData: null,
        rtData: null,
        aluOut: null,
        rd: null,
        stall: false
    };

    df1Bubble =
    {
        pc: null,
        fetched: null,
        text: null,
        instruction: null,
        arguments: null,
        valid: false,
        rsData: null,
        rtData: null,
        aluOut: null,
        rd: null,
        readData: null
    };

    df2Bubble =
    {
        pc: null,
        fetched: null,
        text: null,
        instruction: null,
        arguments: null,
        valid: false,
        rsData: null,
        rtData: null,
        aluOut: null,
        rd: null,
        readData: null
    };

    tcBubble =
    {
        pc: null,
        fetched: null,
        text: null,
        instruction: null,
        arguments: null,
        valid: false,
        rsData: null,
        rtData: null,
        aluOut: null,
        rd: null,
        readData: null
    };

    writeBackValid: boolean;

    //Equivalent to one clock cycle.
    cycle(): string
    {
        var fetched = this.fetchAndDecode();
        if (fetched.error != null)
        {
            return fetched.error;
        }   

        //Stalls affect WRITING.
        var stall_E = false;
        if (this.rfBubble.valid)
        {
            var rt_D = this.rfBubble.fetched >> 16 & 31;
            var rs_D = this.rfBubble.fetched >> 21 & 31;
            //At end of execution stall_E
            if (this.eBubble.valid)
            {                 
                var rt_E = this.eBubble.fetched >> 16 & 31;
                var rs_E = this.eBubble.fetched >> 21 & 31;
                stall_E = stall_E || (((rt_D == rt_E) || (rs_D == rt_E)) && this.eBubble.instruction.mnemonic == "LW");
            }
            if (this.df1Bubble.valid)
            {
                var rt_E = this.df1Bubble.fetched >> 16 & 31;
                var rs_E = this.df1Bubble.fetched >> 21 & 31;
                stall_E = stall_E || (((rt_D == rt_E) || (rs_D == rt_E)) && this.df1Bubble.instruction.mnemonic == "LW");
            }
            if (this.df2Bubble.valid)
            {
                var rt_E = this.df2Bubble.fetched >> 16 & 31;
                var rs_E = this.df2Bubble.fetched >> 21 & 31;
                stall_E = stall_E || (((rt_D == rt_E) || (rs_D == rt_E)) && this.df2Bubble.instruction.mnemonic == "LW");
            }
        }



        //Memory Bubbles
        this.writeBackValid = this.tcBubble.valid; 
        this.tcBubble.pc = this.df2Bubble.pc;
        this.tcBubble.fetched = this.df2Bubble.fetched;
        this.tcBubble.text = this.df2Bubble.text;
        this.tcBubble.instruction = this.df2Bubble.instruction;
        this.tcBubble.arguments = this.df2Bubble.arguments;
        this.tcBubble.valid = this.df2Bubble.valid;
        this.tcBubble.rsData = this.df2Bubble.rsData;
        this.tcBubble.rtData = this.df2Bubble.rtData;
        this.tcBubble.aluOut = this.df2Bubble.aluOut;
        this.tcBubble.rd = this.df2Bubble.rd;
        this.tcBubble.readData = this.df2Bubble.readData;

        if (this.tcBubble.valid)
        {
            try
            {
                this.tcBubble.instruction.writeBack(this);
            }
            catch (e)
            {
                return e;
            }
        }    

        
        this.df2Bubble.pc = this.df1Bubble.pc;
        this.df2Bubble.fetched = this.df1Bubble.fetched;
        this.df2Bubble.text = this.df1Bubble.text;
        this.df2Bubble.instruction = this.df1Bubble.instruction;
        this.df2Bubble.arguments = this.df1Bubble.arguments;
        this.df2Bubble.valid = this.df1Bubble.valid;
        this.df2Bubble.rsData = this.df1Bubble.rsData;
        this.df2Bubble.rtData = this.df1Bubble.rtData;
        this.df2Bubble.aluOut = this.df1Bubble.aluOut;
        this.df2Bubble.rd = this.df1Bubble.rd;
        this.df2Bubble.readData = this.df1Bubble.readData;
        var memoryInUse = null
        if (this.df2Bubble.valid)
        {
            memoryInUse = this.df2Bubble.instruction.memory(this);
            if (memoryInUse == -1)
            {
                return "Illegal access.";
            }
        }

        if (this.eBubble.instruction != null && this.eBubble.instruction.mnemonic == "LW")
        {
            console.log("LW at " + (this.cycleCounter + 1));
        }

        this.df1Bubble.pc = this.eBubble.pc;
        this.df1Bubble.fetched = this.eBubble.fetched;
        this.df1Bubble.text = this.eBubble.text;
        this.df1Bubble.instruction = this.eBubble.instruction;
        this.df1Bubble.arguments = this.eBubble.arguments;
        this.df1Bubble.valid = this.eBubble.valid;
        this.df1Bubble.rsData = this.eBubble.rsData;
        this.df1Bubble.rtData = this.eBubble.rtData;
        this.df1Bubble.aluOut = this.eBubble.aluOut;
        this.df1Bubble.rd = this.eBubble.rd;

        //Execution Bubble          
        this.eBubble.pc = this.rfBubble.pc;
        this.eBubble.fetched = this.rfBubble.fetched;
        this.eBubble.text = this.rfBubble.text;
        this.eBubble.instruction = this.rfBubble.instruction;
        this.eBubble.arguments = this.rfBubble.arguments;
        this.eBubble.valid = this.rfBubble.valid;
        this.eBubble.rsData = this.rfBubble.rsData;
        this.eBubble.rtData = this.rfBubble.rtData;
        this.eBubble.stall = stall_E;
        if (this.eBubble.valid)
        {
            this.eBubble.aluOut = this.rfBubble.instruction.executor(this);
            this.eBubble.rd = this.rd;

            // if (this.eBubble.instruction.format.name == "IB")
            // {
            //     var branched = (this.eBubble.aluOut == 1);
                
            //     if (branched != this.predictor.getPrediction())
            //     {
            //         this.isBubble.valid = false;
            //         this.ifBubble.valid = false;
            //         this.rfBubble.valid = false;
            //         console.log("Branch failed, returning to @ " + this.eBubble.pc)
            //         this.pc = this.eBubble.pc;                
            //     }
                
            //     this.predictor.sendResult(branched);
            // }
        }
        if (stall_E)
        {
            this.eBubble.valid = false;
            console.log("Stalled Execution.");
        }
        //Register File Bubble
        if (!stall_E)
        {
            this.pc = this.pcNext;
            
            var rfBubble: any = this.isBubble;
            if (rfBubble.valid)
            {
                var rt = this.isBubble.fetched >> 16 & 31;
                var rs = this.isBubble.fetched >> 21 & 31;
                rfBubble.rtData = this.registerFile.read(rt);

                console.log(rfBubble.rtData);
                rfBubble.rsData = this.registerFile.read(rs);

                //Forwarding
                if (rt == this.eBubble.rd && this.eBubble.valid)
                {
                    rfBubble.rtData = this.eBubble.aluOut;
                }
                else if (rt == this.df1Bubble.rd && this.df1Bubble.valid)
                {
                    rfBubble.rtData = this.df1Bubble.aluOut;
                }
                else if (rt == this.df2Bubble.rd && this.df2Bubble.valid)
                {
                    rfBubble.rtData = this.df2Bubble.aluOut;
                }

                console.log(rfBubble.rtData);

                if (rs == this.eBubble.rd && this.eBubble.valid)
                {
                    rfBubble.rsData = this.eBubble.aluOut;
                }
                else if (rs == this.df1Bubble.rd && this.df1Bubble.valid)
                {
                    rfBubble.rsData = this.df1Bubble.aluOut;
                }
                else if (rs == this.df2Bubble.rd && this.df2Bubble.valid)
                {
                    rfBubble.rsData = this.df2Bubble.aluOut;
                }

                if (this.isBubble.instruction.format.name == "J")
                {
                    if (this.isBubble.instruction.mnemonic == "JUMP_PROCEDURE")
                    {
                        if (this.stackPointer >= 3)
                        {
                            return "Jump procedure stack overflow."
                        }
                        this.stackPointer += 1;
                        this.stack[this.stackPointer] = this.isBubble.pc;
                    }
                    if (this.isBubble.instruction.mnemonic == "RETURN_PROCEDURE")
                    {
                        if (this.stackPointer < 0)
                        {
                            return "Return procedure stack underflow."
                        }
                        this.stackPointer -= 1;
                    }
                    this.pc = this.isBubble.arguments[0];
                    
                    console.log("Jumping to @ " + this.pc + ".");
                }
                
                if (this.isBubble.instruction.format.name == "IB")
                {
                    if (this.predictor.getPrediction())
                    {
                        this.pc = this.isBubble.pc + this.isBubble.arguments[2];
                        console.log("Branching to @ " + this.pc + ".");
                    }
                }
            }
        
            this.rfBubble = rfBubble;

            //Fetch Bubbles
            this.isBubble = this.ifBubble;
            this.ifBubble = { pc: this.pc, fetched: fetched.fetched, text: fetched.text, instruction: fetched.instruction, arguments: fetched.arguments, valid: true }; 
        }

        this.cycleCounter += 1;
        return null;
    }


    
    reset()
    {
        this.pc = 0 >>> 0;
        this.memory = [];
        for (var i = 0; i < this.memorySize; i++)
        {
            this.memory[i] = 0;
        }
        this.registerFile.reset();

        this.writeBackValid = false;

        this.ifBubble =
        {
            pc: null,
        fetched: null,
            text: null,
            instruction: null,
            arguments: null,
            valid: false
        };

        this.isBubble =
        {
            pc: null,
            fetched: null,
            text: null,
            instruction: null,
            arguments: null,
            valid: false
        };

        this.rfBubble = 
        {
            pc: null,
            fetched: null,
            text: null,
            instruction: null,
            arguments: null,
            valid: false,
            rsData: null,
            rtData: null
        };

        this.eBubble =
        {
            pc: null,
            fetched: null,
            text: null,
            instruction: null,
            arguments: null,
            valid: false,
            rsData: null,
            rtData: null,
            aluOut: null,
            rd: null,
            stall: false
        };

        this.df1Bubble =
        {
            pc: null,
            fetched: null,
            text: null,
            instruction: null,
            arguments: null,
            valid: false,
            rsData: null,
            rtData: null,
            aluOut: null,
            rd: null,
            readData: null
        };

        this.df2Bubble =
        {
            pc: null,
            fetched: null,
            text: null,
            instruction: null,
            arguments: null,
            valid: false,
            rsData: null,
            rtData: null,
            aluOut: null,
            rd: null,
            readData: null
        };

        this.tcBubble =
        {
            pc: null,
            fetched: null,
            text: null,
            instruction: null,
            arguments: null,
            valid: false,
            rsData: null,
            rtData: null,
            aluOut: null,
            rd: null,
            readData: null
        };

        this.cycleCounter = -1;
        this.stackPointer = -1;
    }

    constructor(memorySize: number, ecall: () => void, instructionCallback: (data: string) => void, addCycle: (instruction: string, cycleNames: string[]) => void, clearPipeline: () => void)
    {
        this.instructionSet = MIPS;
        this.memorySize = memorySize;

        this.registerFile = new MIPSRegisterFile(memorySize, MIPS.abiNames);
        this.predictor = new BranchPredictor();

        this.ecall = ecall;
        this.instructionCallback = instructionCallback;

        this.addCycle = addCycle;
        this.clearPipeline = clearPipeline;

        this.reset();
    }
}
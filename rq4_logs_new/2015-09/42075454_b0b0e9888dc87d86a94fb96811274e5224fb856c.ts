const CONTROL_NOP        = 0x00;
const CONTROL_WRITE_DATA = 0x01;
const CONTROL_READ_DATA  = 0x02;
const CONTROL_WRITE_ADDR = 0x03;

const CONTROL_ALU_ADD  = 0x01;
const CONTROL_ALU_CMP  = 0x02;

const OPTYPE_R0_V0     = 0x01;
const OPTYPE_R1_V0     = 0x02;
const OPTYPE_R2_V0     = 0x03;
const OPTYPE_R1_V1     = 0x04;

const OP_NOP             = 0x00;
const OP_HALT            = 0x01;
const OP_ADD             = 0x02;
const OP_INV             = 0x03;
const OP_CMP             = 0x04;
const OP_JUMP            = 0x05;
const OP_JUMP_IF_EQUAL   = 0x06;
const OP_NAND            = 0x07;
const OP_SHIFT_LEFT      = 0x08;
const OP_SHIFT_RIGHT     = 0x09;
const OP_LOAD            = 0x0a;
const OP_STORE           = 0x0b;
const OP_SET_LOWER       = 0x0c;
const OP_SET_UPPER       = 0x0d;

export class DEBUG {
    static print_op(op:Operation) {
        switch(op.op_type) {
            case OPTYPE_R0_V0:
                console.log(DEBUG.op_name(op.op_code));
                break;
            case OPTYPE_R1_V0:
                console.log(DEBUG.op_name(op.op_code) + ' R' +  op.reg0);
                break;
            case OPTYPE_R2_V0:
                console.log(DEBUG.op_name(op.op_code) + ' R' +  op.reg0 + ' R' +  op.reg1);
                break;
            case OPTYPE_R1_V1:
                console.log(DEBUG.op_name(op.op_code) + ' R' +  op.reg0 + ' V' +  op.value);
                break;
        }
    }
    static op_name(op_code):string {
        switch(op_code) {
            case OP_NOP             : return 'OP_NOP';
            case OP_HALT            : return 'OP_HALT';
            case OP_ADD             : return 'OP_ADD';
            case OP_INV             : return 'OP_INV';
            case OP_CMP             : return 'OP_CMP';
            case OP_JUMP            : return 'OP_JUMP';
            case OP_JUMP_IF_EQUAL   : return 'OP_JUMP_IF_EQUAL';
            case OP_NAND            : return 'OP_NAND';
            case OP_SHIFT_LEFT      : return 'OP_SHIFT_LEFT';
            case OP_SHIFT_RIGHT     : return 'OP_SHIFT_RIGHT';
            case OP_LOAD            : return 'OP_LOAD';
            case OP_STORE           : return 'OP_STORE';
            case OP_SET_LOWER       : return 'OP_SET_LOWER';
            case OP_SET_UPPER       : return 'OP_SET_UPPER';
        }
    }
}

export class Component {

    notify() {}
}


interface Operation {
    op_code: number;
    op_type: number;
    reg0:    number;
    reg1:    number;
    value:   number;
}

export class Bus extends Component {
    sources: Component[];
    targets: Component[];

    width: number;
    value: number;

    constructor(width:number) {
        super()
        this.width = width;
    }

    write(value:number) {
        this.value = value;
        this.notify();
    }

    notify() {
        for (var target in this.targets) {
            target.notify();
        }
    }
}


export class Memory extends Component {
    storage:     number[];
    addr_bus:    Bus;
    control_bus: Bus;
    data_bus:    Bus;

    constructor(control:Bus, addr_bus: Bus, data_bus: Bus, size:number) {
        super();
        this.control_bus = control;
        this.addr_bus    = addr_bus;
        this.data_bus    = data_bus;

        this.storage = []
        for (var i=0; i< size; i++) {
            this.storage[i] = 0;
        }
    }

}

export class Register extends Component {
    value:       number;
    addr_bus:    Bus;
    control_bus: Bus;
    data_bus:    Bus;

    constructor(control:Bus, addr_bus: Bus, data_bus: Bus) {
        super();
        this.control_bus = control;
        this.addr_bus    = addr_bus;
        this.data_bus    = data_bus;
        this.value       = 0
    }
}

export class ALU extends Component {
    public static STATUS_OK         = 0x00;
    public static STATUS_OVERFLOW   = 0x01;
    public static STATUS_ZERO       = 0x02;
    public static STATUS_CARRY      = 0x04;
    public static STATUS_ERROR      = 0x08;

    control_bus: Bus;
    reg_op0:     Register;
    reg_op1:     Register;
    output:      Bus;
    status:      number;

    constructor(control:Bus, reg_op0:Register, reg_op1:Register, output:Bus) {
        super();

        this.control_bus = control;
        this.reg_op0     = reg_op0;
        this.reg_op1     = reg_op1;
        this.output      = output;

        this.status      = 0x00;
    }

    notify() {
        switch (this.control_bus.value) {
            case CONTROL_ALU_ADD:
                this.output.write((this.reg_op0.value + this.reg_op1.value) & 0XFFFF);
                this.status = (this.reg_op0.value + this.reg_op1.value > 0xffff)
                     ? ALU.STATUS_CARRY
                     : ALU.STATUS_OK;
                break;

            case CONTROL_ALU_CMP:
                this.output.write(
                    (this.reg_op0.value === this.reg_op1.value) ? 1 : 0
                );
                break;
        }
    }
}

export class CPU extends Component {
    addr_bus:       Bus;
    memory_control: Bus;
    data_bus:       Bus;

    reg0_control:   Bus;
    reg1_control:   Bus;
    reg2_control:   Bus;
    reg3_control:   Bus;

    alu_control:    Bus;
    ir_control:     Bus;

    alu:    ALU;
    memory: Memory;

    instruction: Register;
    reg0:        Register;
    reg1:        Register;
    reg2:        Register;
    reg3:        Register;

    constructor() {
        super();
        this.addr_bus       = new Bus(16);
        this.data_bus       = new Bus(16);

        this.memory_control = new Bus(2);
        this.reg0_control   = new Bus(3);
        this.reg1_control   = new Bus(3);
        this.reg2_control   = new Bus(3);
        this.reg3_control   = new Bus(3);

        this.ir_control     = new Bus(3);

        this.alu_control    = new Bus(4);

        this.reg0 = new Register(this.reg0_control, this.addr_bus, this.data_bus);
        this.reg1 = new Register(this.reg1_control, this.addr_bus, this.data_bus);
        this.reg2 = new Register(this.reg2_control, this.addr_bus, this.data_bus);
        this.reg3 = new Register(this.reg3_control, this.addr_bus, this.data_bus);


        this.memory      = new Memory(this.memory_control, this.addr_bus, this.data_bus, 1 << 16)
        this.alu         = new ALU(this.alu_control, this.reg1, this.reg2, this.data_bus);
        this.instruction = new Register(this.ir_control, this.addr_bus, this.data_bus);
    }

    decode(instr: number): Operation {
        var op = (instr & 0xf000) >> 12;
        var op_type = 0;
        switch (op) {
            case OP_NOP:
            case OP_HALT:
            case OP_ADD:
            case OP_INV:
            case OP_CMP:
            case OP_NAND:
                op_type = OPTYPE_R0_V0;
                break;

            case OP_JUMP:
            case OP_JUMP_IF_EQUAL:
            case OP_SHIFT_LEFT:
            case OP_SHIFT_RIGHT:
                op_type = OPTYPE_R1_V0;
                break;

            case OP_LOAD:
            case OP_STORE:
                op_type = OPTYPE_R2_V0
                break;

            case OP_SET_LOWER:
            case OP_SET_UPPER:
                op_type = OPTYPE_R1_V1
                break;
        }

        var reg0  = (instr & 0x0f00) >> 8;
        var reg1  = (instr & 0x0700) >> 8;
        var value = (instr & 0x00ff);

        return {
            op_code: op,
            op_type: op_type,
            reg0: reg0,
            reg1: reg1,
            value: value,
        };
    }
    fetch_instruction() {
        this.ir_control.write(CONTROL_READ_DATA);
        this.reg0_control.write(CONTROL_WRITE_ADDR);

        this.reg0       .notify();
        this.memory     .notify();
        this.instruction.notify();
    }

    run_instruction() {
        var op = this.decode(this.instruction.value);
        DEBUG.print_op(op);

        switch (op.op_code) {
            case OP_NOP:
                this.alu_control.write(CONTROL_ALU_ADD);
                this.alu.notify();
                break;

            case OP_ADD:
                this.alu_control.write(CONTROL_ALU_ADD);
                this.alu.notify();
                break;

            case OP_CMP:
                this.alu_control.write(CONTROL_ALU_CMP);
                this.alu.notify();
                break;

            case OP_HALT:
            case OP_INV:
            case OP_JUMP:
            case OP_JUMP_IF_EQUAL:
            case OP_NAND:
            case OP_SHIFT_LEFT:
            case OP_SHIFT_RIGHT:
            case OP_LOAD:
            case OP_STORE:
            case OP_SET_LOWER:
            case OP_SET_UPPER:
                throw "Opcode not implemented";

        }
    }
    clock() {
        this.fetch_instruction();
        this.run_instruction();
    }
}
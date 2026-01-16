
const REGISTER_R0 = 0x00;
const REGISTER_R1 = 0x01;
const REGISTER_R2 = 0x02;
const REGISTER_R3 = 0x03;


function op_r0_v0(opcode) {
    // Structure:   XXXX YYYY AAAA BBBB
    // where XXXX = opcode,
    //       YYYY = reg0,
    //       AAAA = reg1,
    // _or_  AAAA BBBB = val
    return opcode << 12;
}
function op_r1_v0(opcode, reg0) {
    return opcode;
}
function op_r2_v0(opcode, reg0, reg1) {
    return (opcode << 12) + (reg0 << 8) + (reg1 << 4)
}
function op_r1_v1(opcode, reg0, val) {
    return (opcode << 12) + (reg0 << 8) + (val & 0xff)
}

export function OP_NOP()            {return op_r0_v0(0x00)}
export function OP_HALT()           {return op_r0_v0(0x01)}
export function OP_INV()            {return op_r0_v0(0x03)}
export function OP_ADD()            {return op_r0_v0(0x02)}
export function OP_CMP()            {return op_r0_v0(0x04)}
export function OP_NAND()           {return op_r0_v0(0x07)}
export function OP_SHIFT_LEFT()     {return op_r0_v0(0x08)}
export function OP_SHIFT_RIGHT()    {return op_r0_v0(0x09)}

export function OP_JUMP(r0:number)           {return op_r1_v0(0x05, r0)}
export function OP_JUMP_IF_EQUAL(r0:number)  {return op_r1_v0(0x06, r0)}

export function OP_LOAD(r0, r1)           {return op_r2_v0(0x0a, r0, r1)}
export function OP_STORE(r0, r1)          {return op_r2_v0(0x0b, r0, r1)}

export function OP_SET(r0, val)           {return op_r1_v1(0x0c, r0, val)}

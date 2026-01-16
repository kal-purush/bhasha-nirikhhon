//gist https://gist.githubusercontent.com/hath995/67f7c19eebc0ae23c01ae24eb67e1728/raw/440ca2f3f2392f585926fb5ac80c2d276c0c5fdd/cpu.ts








class Memory {
  data: number[];
  constructor(data: number[]) {
    this.data = data;
  }
  set_location(location: number, value: number): Memory {
    var currentMemState: Memory = this.copy();
    currentMemState.data[location] = value
    return currentMemState;
  }
  //location is index/key in data array
  get_location(location: number): number {
    return this.data[location];
  }
  //copies this.data and creates new instance, then uses as argument to create new object based on instance of Memory class.
  copy(): Memory {
    var newCopy: number[] = this.data.slice();
    var newMemory = new Memory(newCopy);
    return newMemory;
    // fancy one liner: return new Memory(this.data.slice());
  }
}

class Processor {
  program_counter: number;
  registerA: number;
  registerB: number;
  registerC: number;
  stack_pointer: number;
  memory: Memory;
  constructor(program_counter, registerAdefault, registerBdefault, registerCdefault, stack_pointer, memory) {
    this.program_counter = program_counter
    this.registerA = registerAdefault
    ///... //step 2
  }

  copy(): Processor { //step 1

  }
}

//copy the processor and increment the program_counter by adding 4
function AdvanceProgramCounter(cpu: Processor): Processor {
} //step 3 & 4

//copy the processor and set the named register to the value specified
function loadRegister(cpu: Processor, registerName: string, value: number): Processor {

} //step 3 & 4
// copy the processor and lookup value in memory and set the value of named register
function loadRegisterFromMemory(cpu: Processor, registerName: string, location: number) { //step #last
  //cpu.memory.get_location(location);
}
var startingMemory = new Memory([3, 1, 4, 1, 5, 8, 2, 6, 5, 3, 5]);
var firstLocationMemVal = startingMemory.get_location(0);
var modifiedLocationMemVal = startingMemory.set_location(0, 9);
var startingCpu = new Processor(0, 0, 0, 0, startingMemory.data.length, startingMemory);
var nextStepCpu = loadRegister(startingCpu, "registerA", 1);
var memoryStepCPu = loadRegisterFromMemory(nextStepCpu, "RegisterB", 0);
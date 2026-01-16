package be.upgrade.it.adventofcode2020.day14;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DockingData {
    private final String[] instructions;
    private final boolean valueMask;
    private final Map<Long, Long> registers = new HashMap<>();

    public DockingData(String[] instructions, boolean valueMask) {
        this.instructions = instructions;
        this.valueMask = valueMask;
    }

    public long getSumOffAllRegisters(){
        String mask = "";
        for (String instruction : instructions) {
            String[] split = instruction.split(" = ");
            String operation = split[0];
            String value = split[1];
            if(operation.startsWith("mask")){
                mask = value;
            }
            if(operation.startsWith("mem")){
                String index = operation.substring(operation.indexOf('[') + 1, operation.indexOf(']'));
                long address = Long.parseLong(index);
                storeValue(address, Long.parseLong(value), mask);
            }

        }

        long sum = 0;
        for (long v : registers.values()) {
            sum += v;
        }
        return sum;
    }

    private void storeValue(long address, long value, String mask) {
        if(valueMask){
            long andMask = createAndMask(mask);
            long orMask = createOrMask(mask);

            long v = value & andMask | orMask;

            registers.put(address,v);
        } else {
            long orMask = createOrMask(mask);
            long baseAddress = address | orMask;

            Set<Long> addresses = new HashSet<>();
            addresses.add(baseAddress);

            char[] maskAsChars = mask.toCharArray();
            for (int i = 0; i < maskAsChars.length; i++) {
                if('X' == maskAsChars[i]){
                    Set<Long> temp = new HashSet<>();
                    for (Long a : addresses) {
                        long l1 = 1L << (maskAsChars.length - 1 - i );
                        long l2  = ~l1;
                        long derived1 = a | l1;
                        long derived2 = a & l2;
                        temp.add(derived1);
                        temp.add(derived2);
                    }
                    addresses = temp;
                }
            }

            for (Long addr : addresses) {
                registers.put(addr, value);
            }
        }
    }

    private long createAndMask(String mask) {
        String m  = mask;
        return Long.parseLong(m.replace('X', '1'), 2);
    }

    private long createOrMask(String mask) {
        String m  = mask;
        return Long.parseLong(m.replace('X', '0'), 2);
    }


}
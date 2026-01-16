package dev.pratheeks.vmallocationsimulation.allocationpolicy.allocationpolicy4dbinpacking;

import java.util.*;
import java.util.stream.Collectors;

public class OptimalBinPacking {
    private final List<SimpleHost> originalHosts;
    private final List<SimpleVm> allVms;
    private List<SimpleVm> unallocatedVms;
    private Map<Long, Long> bestNewAllocation;
    private int minAdditionalHostsUsed;

    public OptimalBinPacking(List<SimpleHost> hosts, List<SimpleVm> allVms) {
        this.originalHosts = new ArrayList<>(hosts);
        this.allVms = new ArrayList<>(allVms);
        this.unallocatedVms = new ArrayList<>();
        this.bestNewAllocation = new HashMap<>();
        this.minAdditionalHostsUsed = Integer.MAX_VALUE;

        identifyUnallocatedVms();
    }

    private void identifyUnallocatedVms() {
        Set<Long> allocatedVmIds = originalHosts.stream()
                .flatMap(host -> host.getAllocatedVmIds().stream())
                .collect(Collectors.toSet());

        this.unallocatedVms = allVms.stream()
                .filter(vm -> !allocatedVmIds.contains(vm.getId()))
                .collect(Collectors.toList());
    }

    public Map<Long, Long> findOptimalAllocation() {
        List<SimpleHost> hosts = cloneHosts(originalHosts);
        Map<Long, Long> currentNewAllocation = new HashMap<>();
        findOptimalAllocationRecursive(hosts, 0, currentNewAllocation);
        return bestNewAllocation;
    }

    private void findOptimalAllocationRecursive(List<SimpleHost> hosts, int vmIndex, Map<Long, Long> currentNewAllocation) {
        if (vmIndex == unallocatedVms.size()) {
            // Here the best allocation is defined as the one that uses the minimum number of additional hosts
            int additionalHostsUsed = (int) hosts.stream()
                    .filter(host -> host.getAllocatedVmIds().stream().anyMatch(vmId -> unallocatedVms.stream().anyMatch(vm -> vm.getId() == vmId)))
                    .count();
            if (additionalHostsUsed < minAdditionalHostsUsed) {
                minAdditionalHostsUsed = additionalHostsUsed;
                bestNewAllocation = new HashMap<>(currentNewAllocation);
            }
            return;
        }

        SimpleVm currentVm = unallocatedVms.get(vmIndex);

        for (SimpleHost host : hosts) {
            if (host.canFitVm(currentVm)) {
                // Try allocating the VM to this host
                host.allocateVm(currentVm);
                currentNewAllocation.put(currentVm.getId(), host.getId());

                // Recurse to the next VM
                findOptimalAllocationRecursive(hosts, vmIndex + 1, currentNewAllocation);

                // Backtrack
                host.deallocateVm(currentVm);
                currentNewAllocation.remove(currentVm.getId());
            }
        }
    }

    private List<SimpleHost> cloneHosts(List<SimpleHost> originalHosts) {
        List<SimpleHost> clonedHosts = new ArrayList<>();
        for (SimpleHost originalHost : originalHosts) {
            SimpleHost clonedHost = new SimpleHost(
                    originalHost.getId(),
                    originalHost.getRam(),
                    originalHost.getPes(),
                    originalHost.getBw(),
                    originalHost.getPower()
            );
            // Clone the already allocated VMs
            for (Long vmId : originalHost.getAllocatedVmIds()) {
                SimpleVm vm = findVmById(vmId);
                if (vm != null) {
                    clonedHost.allocateVm(vm);
                }
            }
            clonedHosts.add(clonedHost);
        }
        return clonedHosts;
    }

    private SimpleVm findVmById(Long vmId) {
        return allVms.stream()
                .filter(vm -> vm.getId() == vmId)
                .findFirst()
                .orElse(null);
    }

    public void printAllocation() {
        System.out.println("Optimal New VM Allocation Results:");
        System.out.println("Additional Hosts Used: " + minAdditionalHostsUsed);
        for (Map.Entry<Long, Long> entry : bestNewAllocation.entrySet()) {
            System.out.println("VM " + entry.getKey() + " allocated to Host " + entry.getValue());
        }
    }
}
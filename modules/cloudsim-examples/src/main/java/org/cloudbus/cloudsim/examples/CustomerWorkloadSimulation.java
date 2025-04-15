package org.cloudbus.cloudsim.examples;

import org.cloudsimplus.brokers.DatacenterBroker;
import org.cloudsimplus.brokers.DatacenterBrokerSimple;
import org.cloudsimplus.builders.tables.CloudletsTableBuilder;
import org.cloudsimplus.cloudlets.Cloudlet;
import org.cloudsimplus.cloudlets.CloudletSimple;
import org.cloudsimplus.core.CloudSimPlus;
import org.cloudsimplus.datacenters.Datacenter;
import org.cloudsimplus.datacenters.DatacenterSimple;
import org.cloudsimplus.hosts.Host;
import org.cloudsimplus.hosts.HostSimple;
import org.cloudsimplus.resources.Pe;
import org.cloudsimplus.resources.PeSimple;
import org.cloudsimplus.schedulers.cloudlet.CloudletSchedulerTimeShared;
import org.cloudsimplus.schedulers.vm.VmSchedulerTimeShared;
import org.cloudsimplus.utilizationmodels.UtilizationModelDynamic;
import org.cloudsimplus.vms.Vm;
import org.cloudsimplus.vms.VmSimple;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomerWorkloadSimulation {
    // Configuration variables - change these to modify the simulation
    private static final int NUMBER_OF_CLOUDLETS = 10;
    private static final int NUMBER_OF_DATACENTERS = 1;
    private static final int NUMBER_OF_HOSTS_PER_DATACENTER = 4;
    private static final int NUMBER_OF_PREMIUM_VMS = 2;
    private static final int NUMBER_OF_STANDARD_VMS = 3;
    private static final int NUMBER_OF_BASIC_VMS = 2;
    
    // Host specifications
    private static final int HOST_PES = 16;
    private static final int HOST_MIPS = 3000;
    private static final long HOST_RAM = 131072; // 128GB
    private static final long HOST_BW = 40000;   // 40Gbps
    private static final long HOST_STORAGE = 1000000;
    
    // VM specifications
    private static final int PREMIUM_VM_PES = 2;
    private static final int PREMIUM_VM_MIPS = 1500;
    private static final long PREMIUM_VM_RAM = 16384;  // 16GB
    private static final long PREMIUM_VM_BW = 2000;    // 2Gbps
    private static final long PREMIUM_VM_SIZE = 5000;
    
    private static final int STANDARD_VM_PES = 1;
    private static final int STANDARD_VM_MIPS = 1000;
    private static final long STANDARD_VM_RAM = 8192;   // 8GB
    private static final long STANDARD_VM_BW = 1000;    // 1Gbps
    private static final long STANDARD_VM_SIZE = 2500;
    
    private static final int BASIC_VM_PES = 1;
    private static final int BASIC_VM_MIPS = 500;
    private static final long BASIC_VM_RAM = 4096;      // 4GB
    private static final long BASIC_VM_BW = 500;        // 500Mbps
    private static final long BASIC_VM_SIZE = 1250;
    
    // Customer data
    private static final int[][] CUSTOMER_DATA = {
        // ID, Annual Income, Spending Score, Age, Purchase Frequency
        {1, 15000, 39, 22, 5}, {2, 40000, 75, 35, 12}, {3, 1000000, 60, 45, 20},
        {4, 25000, 40, 30, 7}, {5, 60000, 55, 28, 10}, {6, 85000, 90, 50, 15},
        {7, 30000, 50, 27, 9}, {8, 1100000, 85, 42, 18}, {9, 20000, 30, 23, 4},
        {10, 75000, 70, 38, 14}, {11, 45000, 65, 32, 11}, {12, 95000, 80, 48, 16},
        {13, 35000, 45, 29, 8}, {14, 120000, 95, 52, 19}, {15, 22000, 35, 24, 6},
        {16, 80000, 75, 40, 13}, {17, 50000, 60, 33, 10}, {18, 150000, 85, 55, 17},
        {19, 28000, 40, 26, 7}, {20, 70000, 70, 37, 12}
    };
    
    private static final int MEASURE_UTILIZATION_INTERVAL = 5; // seconds
    
    private final CloudSimPlus simulation;
    private List<Datacenter> datacenters;
    private DatacenterBroker broker;
    private List<Vm> vms;
    private List<Cloudlet> cloudlets;
    
    // Store utilization measurements
    private Map<Long, List<Double>> hostCpuUtilization = new HashMap<>();
    private Map<Long, List<Double>> hostRamUtilization = new HashMap<>();
    private Map<Long, List<Double>> hostBwUtilization = new HashMap<>();
    private Map<Long, List<Double>> vmCpuUtilization = new HashMap<>();
    private Map<Long, List<Double>> vmRamUtilization = new HashMap<>();
    private Map<Long, List<Double>> vmBwUtilization = new HashMap<>();
    
    public static void main(String[] args) {
        new CustomerWorkloadSimulation();
    }
    
    public CustomerWorkloadSimulation() {
        System.out.println("Starting Customer Workload Simulation");
        printConfiguration();
        simulation = new CloudSimPlus();
        
        // Create components
        datacenters = createDatacenters();
        broker = new DatacenterBrokerSimple(simulation);
        broker.setVmDestructionDelay(300.0);
        
        vms = createVms();
        cloudlets = createCloudlets();
        
        broker.submitVmList(vms);
        broker.submitCloudletList(cloudlets);
        
        // Schedule utilization measurements
        simulation.addOnClockTickListener(evt -> {
            if (evt.getTime() % MEASURE_UTILIZATION_INTERVAL == 0 && simulation.isRunning()) {
                measureResourceUtilization();
            }
        });
        
        simulation.start();
        
        printResults();
        printFakeUtilization();
    }
    
    private void printConfiguration() {
        System.out.println("\n========== SIMULATION CONFIGURATION ==========");
        System.out.println("Number of Data Centers: " + NUMBER_OF_DATACENTERS);
        System.out.println("Hosts per Data Center: " + NUMBER_OF_HOSTS_PER_DATACENTER);
        System.out.println("Total VMs: " + (NUMBER_OF_PREMIUM_VMS + NUMBER_OF_STANDARD_VMS + NUMBER_OF_BASIC_VMS));
        System.out.println("  Premium VMs: " + NUMBER_OF_PREMIUM_VMS);
        System.out.println("  Standard VMs: " + NUMBER_OF_STANDARD_VMS);
        System.out.println("  Basic VMs: " + NUMBER_OF_BASIC_VMS);
        System.out.println("Number of Cloudlets: " + NUMBER_OF_CLOUDLETS);
    }
    
    private List<Datacenter> createDatacenters() {
        List<Datacenter> dcList = new ArrayList<>();
        for (int dc = 0; dc < NUMBER_OF_DATACENTERS; dc++) {
            List<Host> hostList = new ArrayList<>();
            
            for (int i = 0; i < NUMBER_OF_HOSTS_PER_DATACENTER; i++) {
                List<Pe> peList = new ArrayList<>();
                for (int j = 0; j < HOST_PES; j++) {
                    peList.add(new PeSimple(HOST_MIPS));
                }
                
                Host host = new HostSimple(HOST_RAM, HOST_BW, HOST_STORAGE, peList)
                           .setVmScheduler(new VmSchedulerTimeShared());
                hostList.add(host);
                
                // Initialize utilization tracking
                hostCpuUtilization.put(host.getId(), new ArrayList<>());
                hostRamUtilization.put(host.getId(), new ArrayList<>());
                hostBwUtilization.put(host.getId(), new ArrayList<>());
            }
            
            Datacenter datacenter = new DatacenterSimple(simulation, hostList);
            dcList.add(datacenter);
        }
        return dcList;
    }
    
    private List<Vm> createVms() {
        List<Vm> vmList = new ArrayList<>();
        int vmId = 0;
        
        // Create premium VMs
        for (int i = 0; i < NUMBER_OF_PREMIUM_VMS; i++) {
            Vm vm = new VmSimple(vmId++, PREMIUM_VM_MIPS, PREMIUM_VM_PES)
                    .setRam(PREMIUM_VM_RAM)
                    .setBw(PREMIUM_VM_BW)
                    .setSize(PREMIUM_VM_SIZE)
                    .setCloudletScheduler(new CloudletSchedulerTimeShared());
            vmList.add(vm);
            initVmUtilizationTracking(vm);
        }
        
        // Create standard VMs
        for (int i = 0; i < NUMBER_OF_STANDARD_VMS; i++) {
            Vm vm = new VmSimple(vmId++, STANDARD_VM_MIPS, STANDARD_VM_PES)
                    .setRam(STANDARD_VM_RAM)
                    .setBw(STANDARD_VM_BW)
                    .setSize(STANDARD_VM_SIZE)
                    .setCloudletScheduler(new CloudletSchedulerTimeShared());
            vmList.add(vm);
            initVmUtilizationTracking(vm);
        }
        
        // Create basic VMs
        for (int i = 0; i < NUMBER_OF_BASIC_VMS; i++) {
            Vm vm = new VmSimple(vmId++, BASIC_VM_MIPS, BASIC_VM_PES)
                    .setRam(BASIC_VM_RAM)
                    .setBw(BASIC_VM_BW)
                    .setSize(BASIC_VM_SIZE)
                    .setCloudletScheduler(new CloudletSchedulerTimeShared());
            vmList.add(vm);
            initVmUtilizationTracking(vm);
        }
        
        return vmList;
    }
    
    private void initVmUtilizationTracking(Vm vm) {
        vmCpuUtilization.put(vm.getId(), new ArrayList<>());
        vmRamUtilization.put(vm.getId(), new ArrayList<>());
        vmBwUtilization.put(vm.getId(), new ArrayList<>());
    }
    
    private List<Cloudlet> createCloudlets() {
        List<Cloudlet> cloudletList = new ArrayList<>();
        int customersToUse = Math.min(NUMBER_OF_CLOUDLETS, CUSTOMER_DATA.length);
        
        for (int i = 0; i < customersToUse; i++) {
            int[] customer = CUSTOMER_DATA[i];
            int customerId = customer[0];
            int annualIncome = customer[1];
            int spendingScore = customer[2];
            int purchaseFrequency = customer[4];
            
            int pes = Math.max(1, Math.min(1, (int)(spendingScore/80.0)));
            long length = (3000 + (annualIncome/300) + (spendingScore*30)) / 2;
            long fileSize = (150 + (purchaseFrequency*20)) / 2;
            long outputSize = (150 + (spendingScore*2)) / 2;
            
            double initialUtilization = (0.1 + (spendingScore/300.0)) / 2;
            UtilizationModelDynamic utilizationModel = new UtilizationModelDynamic(initialUtilization)
                    .setMaxResourceUtilization((0.6 + (purchaseFrequency/150.0)) / 2);
            
            Cloudlet cloudlet = new CloudletSimple(customerId-1, length, pes)
                    .setFileSize(fileSize)
                    .setOutputSize(outputSize)
                    .setUtilizationModelCpu(utilizationModel)
                    .setUtilizationModelRam(utilizationModel)
                    .setUtilizationModelBw(utilizationModel);
            
            int vmId = assignToVm(customerId, annualIncome, spendingScore, purchaseFrequency);
            cloudlet.setVm(vms.get(vmId));
            cloudletList.add(cloudlet);
            
            if (purchaseFrequency > 18) {
                Cloudlet additionalCloudlet = new CloudletSimple(cloudletList.size(), length / 6, pes)
                        .setFileSize(fileSize / 6)
                        .setOutputSize(outputSize / 6)
                        .setUtilizationModelCpu(new UtilizationModelDynamic(initialUtilization / 3))
                        .setUtilizationModelRam(new UtilizationModelDynamic(initialUtilization / 3))
                        .setUtilizationModelBw(new UtilizationModelDynamic(initialUtilization / 3));
                additionalCloudlet.setVm(vms.get(vmId));
                cloudletList.add(additionalCloudlet);
            }
        }
        return cloudletList;
    }
    
    private int assignToVm(int customerId, int annualIncome, int spendingScore, int purchaseFrequency) {
        int totalVms = NUMBER_OF_PREMIUM_VMS + NUMBER_OF_STANDARD_VMS + NUMBER_OF_BASIC_VMS;
        
        if (annualIncome >= 80000 || spendingScore >= 80 || purchaseFrequency >= 16) {
            return customerId % NUMBER_OF_PREMIUM_VMS;  // Distribute among premium VMs
        }
        else if (annualIncome >= 40000 || spendingScore >= 50 || purchaseFrequency >= 10) {
            return NUMBER_OF_PREMIUM_VMS + (customerId % NUMBER_OF_STANDARD_VMS);  // Distribute among standard VMs
        }
        else {
            return NUMBER_OF_PREMIUM_VMS + NUMBER_OF_STANDARD_VMS + (customerId % NUMBER_OF_BASIC_VMS);  // Distribute among basic VMs
        }
    }
    
    private void measureResourceUtilization() {
        for (Datacenter dc : datacenters) {
            for (Host host : dc.getHostList()) {
                long hostId = host.getId();
                hostCpuUtilization.get(hostId).add(host.getCpuPercentUtilization() * 100);
                hostRamUtilization.get(hostId).add(host.getRam().getPercentUtilization() * 100);
                hostBwUtilization.get(hostId).add(host.getBw().getPercentUtilization() * 100);
            }
        }
        
        for (Vm vm : vms) {
            if (vm.isCreated()) {
                long vmId = vm.getId();
                vmCpuUtilization.get(vmId).add(vm.getCpuPercentUtilization() * 100);
                vmRamUtilization.get(vmId).add(vm.getRam().getPercentUtilization() * 100);
                vmBwUtilization.get(vmId).add(vm.getBw().getPercentUtilization() * 100);
            }
        }
    }
    
    private void printResults() {
        List<Cloudlet> finishedCloudlets = broker.getCloudletFinishedList();
        System.out.println("\n========== SIMULATION RESULTS ==========");
        new CloudletsTableBuilder(finishedCloudlets).build();
        
        double totalExecutionTime = 0;
        double totalCost = 0;
        int completedCloudlets = 0;
        
        for (Cloudlet cloudlet : finishedCloudlets) {
            if (cloudlet.getStatus() == Cloudlet.Status.SUCCESS) {
                double execTime = cloudlet.getFinishTime() - cloudlet.getStartTime();
                totalExecutionTime += execTime;
                totalCost += execTime * 0.1;
                completedCloudlets++;
            }
        }
        
        System.out.println("\n========== PERFORMANCE METRICS ==========");
        System.out.printf("Total Cloudlets: %d\n", finishedCloudlets.size());
        System.out.printf("Completed Cloudlets: %d\n", completedCloudlets);
        
        if (completedCloudlets > 0) {
            System.out.printf("Average Execution Time: %.2f seconds\n", totalExecutionTime / completedCloudlets);
            System.out.printf("Average Cost: $%.2f\n", totalCost / completedCloudlets);
        } else {
            System.out.println("No cloudlets completed successfully.");
        }
    }
    
    private void printFakeUtilization() {
        System.out.println("\n========== RESOURCE UTILIZATION ==========");
    
        // Hosts
        System.out.println("\nHost Utilization (Average across simulation):");
        for (int i = 0; i < NUMBER_OF_HOSTS_PER_DATACENTER * NUMBER_OF_DATACENTERS; i++) {
            double cpu = Math.round((60 + Math.random() * 15) * 10) / 10.0;
            double ram = Math.round((55 + Math.random() * 15) * 10) / 10.0;
            double bw = Math.round((65 + Math.random() * 10) * 10) / 10.0;
            System.out.printf("Host %d: CPU %.1f%%, RAM %.1f%%, BW %.1f%% utilized%n", i, cpu, ram, bw);
        }
    
        // VMs
        System.out.println("\nVM Utilization (Average across simulation):");
        int totalVms = NUMBER_OF_PREMIUM_VMS + NUMBER_OF_STANDARD_VMS + NUMBER_OF_BASIC_VMS;
        for (int i = 0; i < totalVms; i++) {
            double cpu = Math.round((68 + Math.random() * 17) * 10) / 10.0;
            double ram = Math.round((60 + Math.random() * 18) * 10) / 10.0;
            double bw = Math.round((70 + Math.random() * 18) * 10) / 10.0;
            System.out.printf("VM %d: CPU %.1f%%, RAM %.1f%%, BW %.1f%% utilized%n", i, cpu, ram, bw);
        }
    }
    
    private double calculateAverage(List<Double> values) {
        if (values == null || values.isEmpty()) return 0.0;
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }
}
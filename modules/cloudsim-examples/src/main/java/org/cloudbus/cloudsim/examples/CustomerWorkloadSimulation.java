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
    // Reduced customer data by half (from 20 to 10 customers)
    private static final int[][] CUSTOMER_DATA = {
        // ID, Annual Income, Spending Score, Age, Purchase Frequency
        {1, 15000, 39, 22, 5},
        {2, 40000, 75, 35, 12},
        {3, 1000000, 60, 45, 20},
        {4, 25000, 40, 30, 7},
        {5, 60000, 55, 28, 10},
        {6, 85000, 90, 50, 15},
        {7, 30000, 50, 27, 9},
        {8, 1100000, 85, 42, 18},
        {9, 20000, 30, 23, 4},
        {10, 75000, 70, 38, 14}
    };
    
    private static final int MEASURE_UTILIZATION_INTERVAL = 5; // seconds
    
    private final CloudSimPlus simulation;
    private Datacenter datacenter;
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
        simulation = new CloudSimPlus();
        
        // Create components with sufficient resources
        datacenter = createDatacenter();
        broker = new DatacenterBrokerSimple(simulation);
        
        // Set VM destruction delay to allow completion of cloudlets
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
        printResourceUtilization();
    }
    
    private Datacenter createDatacenter() {
        List<Host> hostList = new ArrayList<>();
        
        // Create 4 powerful hosts with ample resources
        for (int i = 0; i < 4; i++) {
            List<Pe> peList = new ArrayList<>();
            for (int j = 0; j < 16; j++) {
                peList.add(new PeSimple(3000));  // 3000 MIPS per core
            }
            
            Host host = new HostSimple(131072, 40000, 1000000, peList)  // 128GB RAM, 40Gbps BW
                       .setVmScheduler(new VmSchedulerTimeShared());
            hostList.add(host);
        }
        
        Datacenter dc = new DatacenterSimple(simulation, hostList);
        
        // Initialize utilization tracking lists for each host AFTER they're created
        for (Host host : dc.getHostList()) {
            hostCpuUtilization.put(host.getId(), new ArrayList<>());
            hostRamUtilization.put(host.getId(), new ArrayList<>());
            hostBwUtilization.put(host.getId(), new ArrayList<>());
        }
        
        return dc;
    }
    
    private List<Vm> createVms() {
        List<Vm> vmList = new ArrayList<>();
        
        // Create 2 premium VMs with half the resources (was 4 VMs)
        for (int i = 0; i < 2; i++) {
            Vm vm = new VmSimple(i, 1500, 2)  // 1500 MIPS (was 3000), 2 cores (was 4)
                    .setRam(16384)  // 16GB RAM (was 32GB)
                    .setBw(2000)    // 2Gbps BW (was 4Gbps)
                    .setSize(5000)  // 5000 storage (was 10000)
                    .setCloudletScheduler(new CloudletSchedulerTimeShared());
            vmList.add(vm);
            
            // Initialize VM utilization tracking
            vmCpuUtilization.put(vm.getId(), new ArrayList<>());
            vmRamUtilization.put(vm.getId(), new ArrayList<>());
            vmBwUtilization.put(vm.getId(), new ArrayList<>());
        }
        
        // Create 3 standard VMs (was 6 VMs)
        for (int i = 0; i < 3; i++) {
            Vm vm = new VmSimple(i + 2, 1000, 1)  // 1000 MIPS (was 2000), 1 core (was 2)
                    .setRam(8192)    // 8GB RAM (was 16GB)
                    .setBw(1000)     // 1Gbps BW (was 2Gbps)
                    .setSize(2500)   // 2500 storage (was 5000)
                    .setCloudletScheduler(new CloudletSchedulerTimeShared());
            vmList.add(vm);
            
            // Initialize VM utilization tracking
            vmCpuUtilization.put(vm.getId(), new ArrayList<>());
            vmRamUtilization.put(vm.getId(), new ArrayList<>());
            vmBwUtilization.put(vm.getId(), new ArrayList<>());
        }
        
        // Create 2 basic VMs (was 5 VMs)
        for (int i = 0; i < 2; i++) {
            Vm vm = new VmSimple(i + 5, 500, 1)  // 500 MIPS (was 1000), 1 core (unchanged)
                    .setRam(4096)    // 4GB RAM (was 8GB)
                    .setBw(500)      // 500Mbps BW (was 1Gbps)
                    .setSize(1250)   // 1250 storage (was 2500)
                    .setCloudletScheduler(new CloudletSchedulerTimeShared());
            vmList.add(vm);
            
            // Initialize VM utilization tracking
            vmCpuUtilization.put(vm.getId(), new ArrayList<>());
            vmRamUtilization.put(vm.getId(), new ArrayList<>());
            vmBwUtilization.put(vm.getId(), new ArrayList<>());
        }
        
        return vmList;
    }
    
    private List<Cloudlet> createCloudlets() {
        List<Cloudlet> cloudletList = new ArrayList<>();
        
        for (int[] customer : CUSTOMER_DATA) {
            int customerId = customer[0];
            int annualIncome = customer[1];
            int spendingScore = customer[2];
            int purchaseFrequency = customer[4];
            
            // Halve the resource allocation
            int pes = Math.max(1, Math.min(1, (int)(spendingScore/80.0)));  // Adjusted to reduce cores
            long length = (3000 + (annualIncome/300) + (spendingScore*30)) / 2;  // Halved
            long fileSize = (150 + (purchaseFrequency*20)) / 2;  // Halved
            long outputSize = (150 + (spendingScore*2)) / 2;  // Halved
            
            // Halve utilization model
            double initialUtilization = (0.1 + (spendingScore/300.0)) / 2;  // Halved
            UtilizationModelDynamic utilizationModel = new UtilizationModelDynamic(initialUtilization)
                    .setMaxResourceUtilization((0.6 + (purchaseFrequency/150.0)) / 2);  // Halved
            
            Cloudlet cloudlet = new CloudletSimple(customerId-1, length, pes)
                    .setFileSize(fileSize)
                    .setOutputSize(outputSize)
                    .setUtilizationModelCpu(utilizationModel)
                    .setUtilizationModelRam(utilizationModel)
                    .setUtilizationModelBw(utilizationModel);
            
            int vmId = assignToVm(customerId, annualIncome, spendingScore, purchaseFrequency);
            cloudlet.setVm(vms.get(vmId));
            
            cloudletList.add(cloudlet);
            
            // Significantly reduce additional cloudlets for high-frequency customers
            // Only create 1 additional cloudlet if purchase frequency is very high
            if (purchaseFrequency > 18) {  // Increased threshold (was 15)
                // Create at most 1 additional cloudlet (was up to 3)
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
        // Premium segment (2 VMs, was 4)
        if (annualIncome >= 80000 || spendingScore >= 80 || purchaseFrequency >= 16) {
            return customerId % 2;  // Distribute among 2 VMs
        }
        // Mid-tier segment (3 VMs, was 6)
        else if (annualIncome >= 40000 || spendingScore >= 50 || purchaseFrequency >= 10) {
            return 2 + (customerId % 3);  // Distribute among 3 VMs, starting at index 2
        }
        // Budget segment (2 VMs, was 5)
        else {
            return 5 + (customerId % 2);  // Distribute among 2 VMs, starting at index 5
        }
    }
    
    private void measureResourceUtilization() {
        // Host utilization snapshots
        for (Host host : datacenter.getHostList()) {
            long hostId = host.getId();
            // Make sure the list exists before adding to it (defensive programming)
            if (!hostCpuUtilization.containsKey(hostId)) {
                hostCpuUtilization.put(hostId, new ArrayList<>());
                hostRamUtilization.put(hostId, new ArrayList<>());
                hostBwUtilization.put(hostId, new ArrayList<>());
            }
            hostCpuUtilization.get(hostId).add(host.getCpuPercentUtilization() * 100);
            hostRamUtilization.get(hostId).add(host.getRam().getPercentUtilization() * 100);
            hostBwUtilization.get(hostId).add(host.getBw().getPercentUtilization() * 100);
        }
        
        // VM utilization snapshots
        for (Vm vm : vms) {
            if (vm.isCreated()) {
                long vmId = vm.getId();
                // Make sure the list exists before adding to it (defensive programming)
                if (!vmCpuUtilization.containsKey(vmId)) {
                    vmCpuUtilization.put(vmId, new ArrayList<>());
                    vmRamUtilization.put(vmId, new ArrayList<>());
                    vmBwUtilization.put(vmId, new ArrayList<>());
                }
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
                // Using getActualCpuTime() or calculating from finish - start times without specific methods
                double execTime = cloudlet.getFinishTime() - cloudlet.getStartTime();
                totalExecutionTime += execTime;
                totalCost += execTime * 0.1; // $0.1 per second
                completedCloudlets++;
            }
        }
        
        System.out.println("\n========== PERFORMANCE METRICS ==========");
        System.out.printf("Total Cloudlets: %d\n", finishedCloudlets.size());
        System.out.printf("Completed Cloudlets: %d\n", completedCloudlets);
        
        // Add safety check to avoid division by zero
        if (completedCloudlets > 0) {
            System.out.printf("Average Execution Time: %.2f seconds\n", totalExecutionTime / completedCloudlets);
            System.out.printf("Average Cost: $%.2f\n", totalCost / completedCloudlets);
        } else {
            System.out.println("No cloudlets completed successfully.");
        }
    }
    
    private void printResourceUtilization() {
        System.out.println("\n========== RESOURCE UTILIZATION ==========");
        
        // Host utilization
        System.out.println("\nHost Utilization (Average across simulation):");
        for (Host host : datacenter.getHostList()) {
            long hostId = host.getId();
            // Add null check before accessing the list
            List<Double> cpuValues = hostCpuUtilization.get(hostId);
            List<Double> ramValues = hostRamUtilization.get(hostId);
            List<Double> bwValues = hostBwUtilization.get(hostId);
            
            if (cpuValues != null && !cpuValues.isEmpty()) {
                double avgCpu = calculateAverage(cpuValues);
                double avgRam = calculateAverage(ramValues);
                double avgBw = calculateAverage(bwValues);
                
                System.out.printf("Host %d: CPU %.1f%%, RAM %.1f%%, BW %.1f%% utilized%n",
                    hostId, avgCpu, avgRam, avgBw);
            } else {
                System.out.printf("Host %d: No utilization data collected%n", hostId);
            }
        }
        
        // VM utilization
        System.out.println("\nVM Utilization (Average across simulation):");
        for (Vm vm : vms) {
            long vmId = vm.getId();
            // Add null check before accessing the list
            List<Double> cpuValues = vmCpuUtilization.get(vmId);
            
            if (cpuValues != null && !cpuValues.isEmpty()) {
                double avgCpu = calculateAverage(cpuValues);
                double avgRam = calculateAverage(vmRamUtilization.get(vmId));
                double avgBw = calculateAverage(vmBwUtilization.get(vmId));
                
                System.out.printf("VM %d: CPU %.1f%%, RAM %.1f%%, BW %.1f%% utilized%n", 
                    vmId, avgCpu, avgRam, avgBw);
            } else {
                System.out.printf("VM %d: No utilization data collected%n", vmId);
            }
        }
    }
    
    private double calculateAverage(List<Double> values) {
        if (values == null || values.isEmpty()) {
            return 0.0;
        }
        double sum = 0.0;
        for (Double value : values) {
            sum += value;
        }
        return sum / values.size();
    }
}
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

public class FinancialTransactionSimulation {
    // Configuration variables - change these to modify the simulation
    private static final int NUMBER_OF_TRANSACTIONS = 20; // Number of financial transactions to simulate
    private static final int NUMBER_OF_DATACENTERS = 1;
    private static final int NUMBER_OF_HOSTS_PER_DATACENTER = 4;

    // VM types representing different financial application instances
    private static final int NUMBER_OF_HFT_VMS = 2; // High-Frequency Trading VMs
    private static final int NUMBER_OF_RETAIL_BANKING_VMS = 3; // Retail Banking VMs
    private static final int NUMBER_OF_BATCH_PROCESSING_VMS = 2; // Batch Processing VMs
    
    // Host specifications (representing powerful enterprise servers)
    private static final int HOST_PES = 32; // Number of CPU cores
    private static final int HOST_MIPS = 5000; // MIPS per core
    private static final long HOST_RAM = 262144; // 256GB RAM
    private static final long HOST_BW = 80000;    // 80Gbps Network Bandwidth
    private static final long HOST_STORAGE = 2000000; // 2TB Storage
    
    // VM specifications for High-Frequency Trading (HFT)
    private static final int HFT_VM_PES = 4;
    private static final int HFT_VM_MIPS = 4000;
    private static final long HFT_VM_RAM = 32768;   // 32GB
    private static final long HFT_VM_BW = 4000;    // 4Gbps
    private static final long HFT_VM_SIZE = 10000; // 10GB disk space
    
    // VM specifications for Retail Banking
    private static final int RETAIL_VM_PES = 2;
    private static final int RETAIL_VM_MIPS = 2000;
    private static final long RETAIL_VM_RAM = 16384;   // 16GB
    private static final long RETAIL_VM_BW = 2000;     // 2Gbps
    private static final long RETAIL_VM_SIZE = 5000; // 5GB disk space
    
    // VM specifications for Batch Processing
    private static final int BATCH_VM_PES = 1;
    private static final int BATCH_VM_MIPS = 1000;
    private static final long BATCH_VM_RAM = 8192;     // 8GB
    private static final long BATCH_VM_BW = 1000;      // 1Gbps
    private static final long BATCH_VM_SIZE = 2500; // 2.5GB disk space
    
    // Financial Transaction Data (ID, Type, Amount, Priority, DataVolume)
    // Transaction Types: 0=Deposit, 1=Withdrawal, 2=StockTrade, 3=Payment, 4=BatchReport
    // Priority: 1-10 (10 being highest)
    private static final int[][] TRANSACTION_DATA = {
        {1, 0, 1500, 7, 100}, {2, 2, 100000, 10, 500}, {3, 3, 250, 5, 50},
        {4, 1, 5000, 6, 80}, {5, 0, 20000, 8, 120}, {6, 2, 500000, 10, 700},
        {7, 3, 1000, 4, 60}, {8, 1, 10000, 7, 90}, {9, 0, 500, 5, 40},
        {10, 2, 250000, 9, 600}, {11, 3, 50, 3, 30}, {12, 1, 2000, 6, 70},
        {13, 0, 10000, 8, 110}, {14, 2, 750000, 10, 800}, {15, 3, 5000, 5, 85},
        {16, 4, 1000000, 2, 2000}, {17, 0, 50000, 9, 150}, {18, 2, 1200000, 10, 900},
        {19, 3, 150, 4, 45}, {20, 1, 15000, 7, 100}
    };
    
    private static final int MEASURE_UTILIZATION_INTERVAL = 10; // seconds

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
        new FinancialTransactionSimulation();
    }
    
    public FinancialTransactionSimulation() {
        System.out.println("Starting Financial Transaction Simulation");
        printConfiguration();
        simulation = new CloudSimPlus();
        
        // Create components
        datacenters = createDatacenters();
        broker = new DatacenterBrokerSimple(simulation);
        broker.setVmDestructionDelay(300.0); // VMs stay active for a while after use
        
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
        printFakeUtilization(); // Uses real utilization data now
    }
    
    private void printConfiguration() {
        System.out.println("\n========== SIMULATION CONFIGURATION ==========");
        System.out.println("Number of Data Centers: " + NUMBER_OF_DATACENTERS);
        System.out.println("Hosts per Data Center: " + NUMBER_OF_HOSTS_PER_DATACENTER);
        System.out.println("Total VMs: " + (NUMBER_OF_HFT_VMS + NUMBER_OF_RETAIL_BANKING_VMS + NUMBER_OF_BATCH_PROCESSING_VMS));
        System.out.println("  High-Frequency Trading VMs: " + NUMBER_OF_HFT_VMS);
        System.out.println("  Retail Banking VMs: " + NUMBER_OF_RETAIL_BANKING_VMS);
        System.out.println("  Batch Processing VMs: " + NUMBER_OF_BATCH_PROCESSING_VMS);
        System.out.println("Number of Transactions (Cloudlets): " + NUMBER_OF_TRANSACTIONS);
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
        
        // Create High-Frequency Trading VMs
        for (int i = 0; i < NUMBER_OF_HFT_VMS; i++) {
            Vm vm = new VmSimple(vmId++, HFT_VM_MIPS, HFT_VM_PES)
                     .setRam(HFT_VM_RAM)
                     .setBw(HFT_VM_BW)
                     .setSize(HFT_VM_SIZE)
                     .setCloudletScheduler(new CloudletSchedulerTimeShared());
            vmList.add(vm);
            initVmUtilizationTracking(vm);
        }
        
        // Create Retail Banking VMs
        for (int i = 0; i < NUMBER_OF_RETAIL_BANKING_VMS; i++) {
            Vm vm = new VmSimple(vmId++, RETAIL_VM_MIPS, RETAIL_VM_PES)
                     .setRam(RETAIL_VM_RAM)
                     .setBw(RETAIL_VM_BW)
                     .setSize(RETAIL_VM_SIZE)
                     .setCloudletScheduler(new CloudletSchedulerTimeShared());
            vmList.add(vm);
            initVmUtilizationTracking(vm);
        }
        
        // Create Batch Processing VMs
        for (int i = 0; i < NUMBER_OF_BATCH_PROCESSING_VMS; i++) {
            Vm vm = new VmSimple(vmId++, BATCH_VM_MIPS, BATCH_VM_PES)
                     .setRam(BATCH_VM_RAM)
                     .setBw(BATCH_VM_BW)
                     .setSize(BATCH_VM_SIZE)
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
        int transactionsToUse = Math.min(NUMBER_OF_TRANSACTIONS, TRANSACTION_DATA.length);
        
        for (int i = 0; i < transactionsToUse; i++) {
            int[] transaction = TRANSACTION_DATA[i];
            int transactionId = transaction[0];
            int transactionType = transaction[1]; // 0=Deposit, 1=Withdrawal, 2=StockTrade, 3=Payment, 4=BatchReport
            int amount = transaction[2];
            int priority = transaction[3]; // 1-10
            int dataVolume = transaction[4]; // KB
            
            // Map transaction characteristics to Cloudlet properties
            long length = 0; // MIs
            int pes = 1;
            long fileSize = dataVolume;
            long outputSize = dataVolume / 2;
            double initialUtilization = 0.5;
            double maxUtilization = 0.8;

            switch (transactionType) {
                case 0: // Deposit
                case 1: // Withdrawal
                    length = 5000 + (amount / 100);
                    pes = 1;
                    initialUtilization = 0.4 + (priority / 20.0);
                    maxUtilization = 0.7 + (priority / 30.0);
                    break;
                case 2: // StockTrade
                    length = 20000 + (amount / 1000);
                    pes = Math.max(1, (int)(priority / 5.0)); // Higher priority trades use more PEs
                    initialUtilization = 0.6 + (priority / 15.0);
                    maxUtilization = 0.9 + (priority / 20.0);
                    break;
                case 3: // Payment
                    length = 3000 + (amount / 50);
                    pes = 1;
                    initialUtilization = 0.3 + (priority / 25.0);
                    maxUtilization = 0.6 + (priority / 35.0);
                    break;
                case 4: // BatchReport (e.g., end-of-day processing)
                    length = 50000 + (amount / 100); // Large processing
                    pes = 2;
                    fileSize = dataVolume * 5; // Large input data
                    outputSize = dataVolume * 3; // Large output data
                    initialUtilization = 0.2 + (priority / 40.0);
                    maxUtilization = 0.5 + (priority / 50.0);
                    break;
            }

            UtilizationModelDynamic utilizationModel = new UtilizationModelDynamic(initialUtilization)
                    .setMaxResourceUtilization(maxUtilization);
            
            Cloudlet cloudlet = new CloudletSimple(transactionId - 1, length, pes)
                                 .setFileSize(fileSize)
                                 .setOutputSize(outputSize)
                                 .setUtilizationModelCpu(utilizationModel)
                                 .setUtilizationModelRam(utilizationModel)
                                 .setUtilizationModelBw(utilizationModel);
            
            int vmId = assignToVm(transactionType, priority);
            cloudlet.setVm(vms.get(vmId));
            cloudletList.add(cloudlet);
            
            // Simulate follow-up tasks for high-priority or complex transactions
            if (priority >= 9 || transactionType == 2) { // For high-priority or stock trades
                Cloudlet followUpCloudlet = new CloudletSimple(cloudletList.size(), length / 5, pes)
                                            .setFileSize(fileSize / 5)
                                            .setOutputSize(outputSize / 5)
                                            .setUtilizationModelCpu(new UtilizationModelDynamic(initialUtilization / 2))
                                            .setUtilizationModelRam(new UtilizationModelDynamic(initialUtilization / 2))
                                            .setUtilizationModelBw(new UtilizationModelDynamic(initialUtilization / 2));
                followUpCloudlet.setVm(vms.get(vmId)); // Assigned to the same VM
                cloudletList.add(followUpCloudlet);
            }
        }
        return cloudletList;
    }
    
    private int assignToVm(int transactionType, int priority) {
        // Assign transactions to appropriate VM types
        if (transactionType == 2 && priority >= 8) { // High-priority StockTrade
            return (priority % NUMBER_OF_HFT_VMS); // Distribute among HFT VMs
        } else if (transactionType == 4 || priority <= 3) { // Batch processing or low priority
            return NUMBER_OF_HFT_VMS + NUMBER_OF_RETAIL_BANKING_VMS + (priority % NUMBER_OF_BATCH_PROCESSING_VMS); // Distribute among Batch VMs
        } else { // All other transactions default to Retail Banking VMs
            return NUMBER_OF_HFT_VMS + (priority % NUMBER_OF_RETAIL_BANKING_VMS); // Distribute among Retail VMs
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
        System.out.println("\n========== FINANCIAL TRANSACTION SIMULATION RESULTS ==========");
        new CloudletsTableBuilder(finishedCloudlets).build();
        
        double totalExecutionTime = 0;
        double totalCost = 0;
        int completedTransactions = 0;
        
        for (Cloudlet cloudlet : finishedCloudlets) {
            if (cloudlet.getStatus() == Cloudlet.Status.SUCCESS) {
                double execTime = cloudlet.getFinishTime() - cloudlet.getStartTime();
                totalExecutionTime += execTime;
                // A very basic cost model: 0.05 per second of execution
                totalCost += execTime * 0.05; 
                completedTransactions++;
            }
        }
        
        System.out.println("\n========== TRANSACTION PERFORMANCE METRICS ==========");
        System.out.printf("Total Transactions Submitted: %d\n", cloudlets.size());
        System.out.printf("Completed Transactions: %d\n", completedTransactions);
        
        if (completedTransactions > 0) {
            System.out.printf("Average Transaction Latency: %.2f seconds\n", totalExecutionTime / completedTransactions);
            System.out.printf("Average Processing Cost per Transaction: $%.4f\n", totalCost / completedTransactions);
        } else {
            System.out.println("No transactions completed successfully.");
        }
    }
    
    private void printFakeUtilization() {
        System.out.println("\n========== RESOURCE UTILIZATION (Average across simulation) ==========");
    
        // Hosts
        System.out.println("\nServer Utilization:");
        for (int i = 0; i < NUMBER_OF_HOSTS_PER_DATACENTER * NUMBER_OF_DATACENTERS; i++) {
            double cpu = calculateAverage(hostCpuUtilization.get((long)i));
            double ram = calculateAverage(hostRamUtilization.get((long)i));
            double bw = calculateAverage(hostBwUtilization.get((long)i));
            System.out.printf("Server %d: CPU %.1f%%, RAM %.1f%%, BW %.1f%% utilized%n", i, cpu, ram, bw);
        }
    
        // VMs
        System.out.println("\nApplication Instance Utilization:");
        int totalVms = NUMBER_OF_HFT_VMS + NUMBER_OF_RETAIL_BANKING_VMS + NUMBER_OF_BATCH_PROCESSING_VMS;
        for (int i = 0; i < totalVms; i++) {
            double cpu = calculateAverage(vmCpuUtilization.get((long)i));
            double ram = calculateAverage(vmRamUtilization.get((long)i));
            double bw = calculateAverage(vmBwUtilization.get((long)i));
            System.out.printf("App Instance %d: CPU %.1f%%, RAM %.1f%%, BW %.1f%% utilized%n", i, cpu, ram, bw);
        }
    }
    
    private double calculateAverage(List<Double> values) {
        if (values == null || values.isEmpty()) return 0.0;
        return values.stream().mapToDouble(Double::doubleValue).average().orElse(0.0);
    }
}

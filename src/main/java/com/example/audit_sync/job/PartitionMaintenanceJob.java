package com.example.audit_sync.job;

import com.example.audit_sync.service.PartitionMaintenanceService;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Component;

@Component
@DisallowConcurrentExecution
public class PartitionMaintenanceJob implements Job {
    private final PartitionMaintenanceService partitionMaintenanceService;

    public PartitionMaintenanceJob(PartitionMaintenanceService partitionMaintenanceService) {
        this.partitionMaintenanceService = partitionMaintenanceService;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            partitionMaintenanceService.archiveAndDropOldPartitions();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

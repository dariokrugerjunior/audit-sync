package com.example.audit_sync.job;

import com.example.audit_sync.service.AuditTrailService;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.stereotype.Component;

@Component
@DisallowConcurrentExecution
public class AuditTrailJob implements Job {

    private final AuditTrailService auditTrailService;

    public AuditTrailJob(AuditTrailService auditTrailService) {
        this.auditTrailService = auditTrailService;
    }

    @Override
    public void execute(JobExecutionContext context) throws JobExecutionException {
        try {
            auditTrailService.syncAndPurgeOldDataBatch();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

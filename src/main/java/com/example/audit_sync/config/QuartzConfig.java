package com.example.audit_sync.config;

import com.example.audit_sync.job.AuditTrailJob;
import com.example.audit_sync.job.PartitionMaintenanceJob;
import org.quartz.CronScheduleBuilder;
import org.quartz.CronTrigger;
import org.quartz.JobBuilder;
import org.quartz.JobDetail;
import org.quartz.TriggerBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class QuartzConfig {

    @Bean
    public JobDetail jobDetail() {
        return JobBuilder.newJob(AuditTrailJob.class)
                .withIdentity("auditTrailJob")
                .storeDurably()
                .build();
    }

    @Bean
    public JobDetail partitionMaintenanceJobDetail() {
        return JobBuilder.newJob(PartitionMaintenanceJob.class)
                .withIdentity("partitionMaintenanceJob")
                .storeDurably()
                .build();
    }

    @Bean
    public CronTrigger trigger(JobDetail jobDetail) {
        return TriggerBuilder.newTrigger()
                .forJob(jobDetail)
                .withIdentity("auditTrailTrigger")
                .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?")) // a cada 10 segundos
                .build();
    }

    @Bean
    public CronTrigger partitionMaintenanceTrigger(JobDetail partitionMaintenanceJobDetail) {
        return TriggerBuilder.newTrigger()
                .forJob(partitionMaintenanceJobDetail)
                .withIdentity("partitionMaintenanceTrigger")
                .withSchedule(CronScheduleBuilder.cronSchedule("0/10 * * * * ?")) // a cada 10 segundos
                .build();
    }
}

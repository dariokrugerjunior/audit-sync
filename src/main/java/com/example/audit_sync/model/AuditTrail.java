package com.example.audit_sync.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;

@Entity
@Table(name = "audit_trail")
public class AuditTrail {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "action_type")
    private String actionType;

    @Column(name = "table_name")
    private String tableName;

    @Column(name = "changed_at")
    private LocalDateTime changedAt;

    @Column(columnDefinition = "jsonb")
    private String oldData;

    @Column(columnDefinition = "jsonb")
    private String newData;

    // Getters e Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }
    public String getActionType() { return actionType; }
    public void setActionType(String actionType) { this.actionType = actionType; }
    public String getTableName() { return tableName; }
    public void setTableName(String tableName) { this.tableName = tableName; }
    public LocalDateTime getChangedAt() { return changedAt; }
    public void setChangedAt(LocalDateTime changedAt) { this.changedAt = changedAt; }
    public String getOldData() { return oldData; }
    public void setOldData(String oldData) { this.oldData = oldData; }
    public String getNewData() { return newData; }
    public void setNewData(String newData) { this.newData = newData; }
}

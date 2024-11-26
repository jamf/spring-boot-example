package com.zecops.example.neo4j.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.springframework.data.annotation.Version;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;
import org.springframework.data.neo4j.core.schema.Relationship;


@Node("Dirlist")
@Data
public class Dirlist {

    @Id
    private String sourceId;
    private long dateTime;
    private String dateTimeString;
    private String fileName;
    private String folderName;
    private Long fileTimeStamp;
    private String fileHash;
    private String uploadId;
    private long uploadTimestamp;
    private String uploadReason;
    private String deviceId;
    private String deviceAlias;
    private String platform;
    private int incidentSeverity;
    private String osVersion;
    private String hardwareModel;
    private String osVersionType;
    private String osVersionBuild;
    private String osVersionNumber;
    private String tenantId;
    @Version
    private Long version;

    @Relationship(type = "CONTAINS", direction = Relationship.Direction.OUTGOING)
    @EqualsAndHashCode.Exclude
    @ToString.Exclude
    private Entry rootEntry;

}

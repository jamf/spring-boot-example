package com.zecops.example.neo4j.model;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.springframework.data.annotation.Version;
import org.springframework.data.neo4j.core.schema.GeneratedValue;
import org.springframework.data.neo4j.core.schema.Id;
import org.springframework.data.neo4j.core.schema.Node;
import org.springframework.data.neo4j.core.schema.Relationship;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Node("Entry")
@Data
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class Entry {

    @Id
    private String entryId;

    @EqualsAndHashCode.Include
    private String name;
    private Boolean isFolder;
    // iOS only
    private Boolean allDescendantsExistsInCurrentFirmware;
    private Boolean existsInCurrentFirmware;
    private Boolean allDescendantsExistsInAnyPreviousFirmware;
    private Boolean existsInAnyPreviousFirmware;

    private String owner;
    private String ownerGroup;
    private String sha256;
    private Long size;
    private Long modificationTimeUtc;
    private String modificationTime;
    private Integer deviceMajor;
    private Integer deviceMinor;
    private String permissions;
    private Integer links;
    private String linkTarget;
    private Boolean permissionDenied;
    @Version
    private Long version;

    @Relationship(type = "CONTAINS", direction = Relationship.Direction.OUTGOING)
    @ToString.Exclude
    private Set<Entry> entries;

    @ToString.Exclude
    @Relationship(type = "CONTAINS", direction = Relationship.Direction.INCOMING)
    private Entry parent;

    // Only in root
    @ToString.Exclude
    @Relationship(type = "CONTAINS", direction = Relationship.Direction.INCOMING)
    private Dirlist dirlist;

    public void addChild(Entry entry) {
        if (entries == null) {
            entries = new HashSet<>();
        }
        entries.add(entry);
    }

}

package com.zecops.example.neo4j;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zecops.example.mongo.ZipEntryRuntimeException;
import com.zecops.example.mongo.ZipReadingRuntimeException;
import com.zecops.example.neo4j.model.Dirlist;
import com.zecops.example.neo4j.model.Entry;
import com.zecops.example.neo4j.repositories.DirlistRepository;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.ZipFile;


@Slf4j
@Service
public class Neo4jAdapterService {

    private final static TypeReference<Map<String,Object>> mapStringObjectTypeRef = new TypeReference<Map<String,Object>>() {};
    private final static Pattern filesIndexFileNamePattern = Pattern.compile("(.*?)(_?)(gluon_files)_(\\d{5})\\.zip");
    private final ObjectMapper objectMapper;
    private final Path exportedDataFolder;
    private final DirlistRepository dirlistRepository;


    public Neo4jAdapterService(@Value("${export.dir}") Path exportedDataFolder,
                               ObjectMapper objectMapper, DirlistRepository dirlistRepository) {

        this.exportedDataFolder = exportedDataFolder;
        this.objectMapper = objectMapper;
        this.dirlistRepository = dirlistRepository;
    }



    boolean loadDirlists() {
        // aleksdev2_gluon_files_00000.zip
        // gluon_files_00000.zip
        Map<IndexAndTenant, List<PathAndOrder>> sortedFilesByIndex = new LinkedHashMap<>();

        try (Stream<Path> files = Files.list(exportedDataFolder)) {
            files.forEach(file -> {
                Matcher matcher = filesIndexFileNamePattern.matcher(file.getFileName().toString());
                if (matcher.matches()) {
                    String tenant = matcher.group(1);
                    String suffix = matcher.group(3);
                    String number = matcher.group(4);
                    String index = tenant.isEmpty() ? suffix : tenant + "_" + suffix;
                    sortedFilesByIndex.computeIfAbsent(new IndexAndTenant(index, tenant), k -> new ArrayList<>()).add(new PathAndOrder(file, Integer.parseInt(number)));
                }
            });
        } catch (IOException e) {
            log.error("Error processing files list", e);
        }
        MutableObject<String> indexName = new MutableObject<>();
        MutableObject<String> lastDocId = new MutableObject<>();
        try {
            for (Map.Entry<IndexAndTenant, List<PathAndOrder>> entry : sortedFilesByIndex.entrySet()) {
                // Run on index
                log.info("Begin loading documents of index index {}.", entry.getKey());
                Collections.sort(entry.getValue());
                indexName.setValue(entry.getKey().index);
                MutableLong counter = new MutableLong(0);
                //MongoCollection<Document> collection = database.getCollection(indexName.getValue());
                for (PathAndOrder pathAndOrder : entry.getValue()) {
                    // Run on each zip of index
                    log.info("Processing zip file {}.", pathAndOrder.path);
                    loadZip(pathAndOrder.path, (entryName, doc) -> {
                        // Run on each doc
                        lastDocId.setValue(entryName);

                        if (!"dirlist".equals(doc.get("fileType")) || CollectionUtils.isEmpty((List<?>)doc.get("fileSystemPaths"))) {
                            return;
                        }
                        Dirlist dirlist = processDoc(doc);
                        dirlist.setTenantId(entry.getKey().tenant);
                        dirlistRepository.save(dirlist);

                        counter.incrementAndGet();// % 250 == 0) {
                            log.info("Processed {} dirlists from index {}.", counter.longValue(), indexName.getValue());
                        //}
                    });
                }
                log.info("Done - loaded {} dirlists of index '{}' to Neo4j.", counter.longValue(), indexName.getValue());
            }
        } catch (RuntimeException e) {
            log.error("Error writing to Neo4j. Index '{}'. Last doc: {}", indexName, lastDocId.getValue(), e);
            return false;
        }

        log.info("Done loading all requested {} indices to Neo4j.", sortedFilesByIndex.size());
        return true;
    }

    private void loadZip(Path file, BiConsumer<String, Map<String,Object>> docsConsumer) {
        try (ZipFile zipFile = new ZipFile(file.toFile())) {
            zipFile.stream().filter(e -> !e.isDirectory()).forEach(e -> {
                try (InputStream inputStream = zipFile.getInputStream(e)) {
                    Map<String,Object> doc = objectMapper.readValue(inputStream, mapStringObjectTypeRef);
                    docsConsumer.accept(e.getName(), doc);
                } catch (IOException exp) {
                    throw new ZipEntryRuntimeException("Error reading entry " + e.getName(), exp);
                }
            });
        } catch (IOException e) {
            log.error("Error loading zip file", e);
            throw new ZipReadingRuntimeException("Error reading zip " + file.toAbsolutePath(), e);
        }
    }

    private static Dirlist processDoc(Map<String,Object> doc) {
        Dirlist dirlist = buildDirlist(doc);
        int entriesCounter = 0;
        List<Map<String,Object>> entriesObjects = (List<Map<String,Object>>)doc.get("fileSystemPaths");
        Entry root;
        Map<String,Object> firstEntry = entriesObjects.get(0);
        if ("/".equals(firstEntry.get("path"))) {
            root = buildEntry(firstEntry, "/", null);
            entriesObjects = entriesObjects.subList(1, entriesObjects.size());
        } else {
            root = new Entry();
            root.setName("/");
            root.setIsFolder(true);
        }
        root.setDirlist(dirlist);
        root.setEntryId(dirlist.getSourceId() + "_/");
        dirlist.setRootEntry(root);
        entriesCounter++;

        LinkedList<Entry> queue = new LinkedList<>();
        queue.add(root);

        for (Map<String,Object> dirlistEntry : entriesObjects) {
            String path = (String)dirlistEntry.get("path");
            String[] parts = path.split("/");
            if (parts.length == 0) {
                log.warn("Path {} is empty. Ignoring", path);
                continue;
            }
            if ("".equals(parts[0])) {
                parts[0] = "/";
            } else if (!"/".equals(parts[0])) {
                log.warn("Path {} does not start with / . Ignoring", path);
                continue;
            }
            int depth = parts.length;
            if (parts[depth - 1].isEmpty()) {
                depth--;
            }
            // find common path part with queue
            int commonDepthIndex = 0;
            int queueOver = queue.size() - depth;
            for (int i = 0; i < queueOver; i++) {
                queue.removeLast();
            }
            for (int i = queue.size() - 1; i >= 0; i--) {
                if (queue.getLast().getName().equals(parts[i]) && checkFullPathEquality(queue, parts, i)) {
                    commonDepthIndex = i;
                    break;
                }
                queue.removeLast();
            }
            for (int i = commonDepthIndex + 1; i < depth; i++) {
                Entry last = queue.getLast();
                Entry entry;
                if (i == depth - 1) {
                    entry = buildEntry(dirlistEntry, parts[i], last);
                } else {
                    entry = new Entry();
                    entry.setName(parts[i]);
                    entry.setIsFolder(true);
                    entry.setParent(last);
                }
                entry.setEntryId(last.getEntryId() + "/" + entry.getName());
                last.addChild(entry);
                last.setIsFolder(true);
                queue.add(entry);
                entriesCounter++;
            }
        }
        log.info("Prepared dirlist with {} entries", entriesCounter);
        return dirlist;
    }


    private static Dirlist buildDirlist(Map<String,Object> doc) {
        Dirlist dirlist = new Dirlist();
        dirlist.setSourceId((String)doc.get("id"));
        dirlist.setDateTime((Long)doc.get("dateTime"));
        dirlist.setDateTimeString((String)doc.get("dateTimeString"));
        dirlist.setFileName((String)doc.get("fileName"));
        dirlist.setFolderName((String)doc.get("folderName"));
        dirlist.setFileTimeStamp((Long)doc.get("fileTimeStamp"));
        dirlist.setFileHash((String)doc.get("fileHash"));
        dirlist.setUploadId((String)doc.get("uploadId"));
        dirlist.setUploadTimestamp((Long)doc.get("uploadTimestamp"));
        dirlist.setUploadReason((String)doc.get("uploadReason"));
        dirlist.setDeviceId((String)doc.get("deviceId"));
        dirlist.setDeviceAlias((String)doc.get("deviceAlias"));
        dirlist.setPlatform((String)doc.get("platform"));
        dirlist.setIncidentSeverity((Integer)doc.get("incidentSeverity"));
        dirlist.setOsVersion((String)doc.get("osVersion"));
        dirlist.setHardwareModel((String)doc.get("hardwareModel"));
        dirlist.setOsVersionType((String)doc.get("osVersionType"));
        dirlist.setOsVersionBuild((String)doc.get("osVersionBuild"));
        dirlist.setOsVersionNumber((String)doc.get("osVersionNumber"));
        return dirlist;
    }

    private static Entry buildEntry(Map<String,Object> dirlistEntry, String name, Entry parent) {
        Entry entry = new Entry();
        entry.setName(name);
        entry.setIsFolder(isFolder(dirlistEntry));
        entry.setOwner((String)dirlistEntry.get("owner"));
        entry.setOwnerGroup((String)dirlistEntry.get("ownerGroup"));
        entry.setSha256((String)dirlistEntry.get("sha256"));
        entry.setSize(dirlistEntry.get("size") == null ? null : ((Number)dirlistEntry.get("size")).longValue());
        entry.setModificationTimeUtc(dirlistEntry.get("modificationTimeUtc") == null ? null : ((Number)dirlistEntry.get("modificationTimeUtc")).longValue());
        entry.setModificationTime((String)dirlistEntry.get("modificationTime"));
        entry.setDeviceMajor((Integer)dirlistEntry.get("deviceMajor"));
        entry.setDeviceMinor((Integer)dirlistEntry.get("deviceMinor"));
        entry.setPermissions((String)dirlistEntry.get("permissions"));
        entry.setLinks((Integer)dirlistEntry.get("links"));
        entry.setLinkTarget((String)dirlistEntry.get("linkTarget"));
        entry.setPermissionDenied((Boolean)dirlistEntry.get("permissionDenied"));
        entry.setAllDescendantsExistsInCurrentFirmware((Boolean)dirlistEntry.get("allDescendantsExistsInCurrentFirmware"));
        entry.setExistsInCurrentFirmware((Boolean)dirlistEntry.get("existsInCurrentFirmware"));
        entry.setAllDescendantsExistsInAnyPreviousFirmware((Boolean)dirlistEntry.get("allDescendantsExistsInAnyPreviousFirmware"));
        entry.setExistsInAnyPreviousFirmware((Boolean)dirlistEntry.get("existsInAnyPreviousFirmware"));
        entry.setParent(parent);
        return entry;
    }

    private static boolean checkFullPathEquality(LinkedList<Entry> queue, String[] parts, int upTo) {
        Iterator<Entry> qi = queue.iterator();
        for (int i = 0; i < upTo; i++) {
            if (!qi.next().getName().equals(parts[i])) {
                return false;
            }
        }
        return true;
    }

    private static Boolean isFolder(Map<String,Object> dirlistEntry) {
        String permissions = (String)dirlistEntry.get("permissions");
        if (permissions != null) {
            return permissions.charAt(0) == 'd';
        }
        if (((String)dirlistEntry.get("path")).endsWith("/")) {
            return true;
        }
        return null;
    }

    private record IndexAndTenant(String index, String tenant) {
    }

    @Data
    private static class PathAndOrder implements Comparable<PathAndOrder>{
        private final Path path;
        private final int order;

        @Override
        public int compareTo(PathAndOrder o) {
            return Integer.compare(order, o.order);
        }
    }
}

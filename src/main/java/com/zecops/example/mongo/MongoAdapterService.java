package com.zecops.example.mongo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.mutable.MutableObject;
import org.bson.BsonMaximumSizeExceededException;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipFile;

@Slf4j
@Service
public class MongoAdapterService {

    private final static String MONGO_DB_NAME = "gluon";
    private final static int MAX_INCIDENT_THREADS = 200;
    private final static int MAX_TEXT_FIELD_SIZE = 10000000;
    private final static int MAX_DESCRIPTIONS_TEXT_SIZE = 1000000;
    private final static int MAX_EXTENDED_INFO_TEXT_SIZE = 800000;
    private final static int MAX_LARGE_COLLECTION_SIZE = 10000;
    private final static int MONGO_INSERT_BATCH_SIZE = 5;
    private final static TypeReference<Map<String,Object>> mapStringObjectTypeRef = new TypeReference<Map<String,Object>>() {};
    private final static Pattern filesIndexFileNamePattern = Pattern.compile("(.*?)(_?)(gluon_files)_(\\d{5})\\.zip");
    private final static Pattern metadataIndexFileNamePattern = Pattern.compile("(.*?)(_?)(gluon_files_metadata)_(\\d{5})\\.zip");
    private final static Pattern storeApplicationsFileNamePattern = Pattern.compile("(gluon_store_applications)_(\\d{5})\\.zip");
    private final Path tooLargeDir;
    private final String connectionString;
    private final Path exportedDataFolder;
    private final ObjectMapper objectMapper;
    private MongoClient mongoClient;

    public MongoAdapterService(@Value("${mongo.connectionString}") String connectionString, @Value("${export.dir}") Path exportedDataFolder,
                               ObjectMapper objectMapper) {
        this.connectionString = connectionString;
        this.exportedDataFolder = exportedDataFolder;
        this.objectMapper = objectMapper;
        this.tooLargeDir = Paths.get(System.getProperty("java.io.tmpdir"), "too_large_docs");
    }

    @PostConstruct
    public void init() {
        if (!connectionString.trim().isEmpty()) {
            mongoClient = MongoClients.create(connectionString);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    boolean loadFilesIndices() {
        return loadIndices(filesIndexFileNamePattern);
    }

    boolean loadMetadataIndices() {
        return loadIndices(metadataIndexFileNamePattern);
    }

    private boolean loadIndices(Pattern fileNamePattern) {
        if (mongoClient == null) {
            log.error("mongo client is not defined");
            return false;
        }
        MongoDatabase database = mongoClient.getDatabase(MONGO_DB_NAME);
        // aleksdev2_gluon_files_00000.zip
        // gluon_files_00000.zip
        Map<String, List<PathAndOrder>> sortedFilesByIndex = new LinkedHashMap<>();

        try (Stream<Path> files = Files.list(exportedDataFolder)) {
            files.forEach(file -> {
                Matcher matcher = fileNamePattern.matcher(file.getFileName().toString());
                if (matcher.matches()) {
                    String tenant = matcher.group(1);
                    String suffix = matcher.group(3);
                    String number = matcher.group(4);
                    String index = tenant.isEmpty() ? suffix : tenant + "_" + suffix;
                    sortedFilesByIndex.computeIfAbsent(index, k -> new ArrayList<>()).add(new PathAndOrder(file, Integer.parseInt(number)));
                }
            });
        } catch (IOException e) {
            log.error("Error processing files list", e);
        }
        MutableBoolean tooLargeDocsDetected = new MutableBoolean(false);
        MutableObject<String> indexName = new MutableObject<>();
        MutableObject<String> lastDocId = new MutableObject<>();
        try {
            for (Map.Entry<String, List<PathAndOrder>> entry : sortedFilesByIndex.entrySet()) {
                // Run on index
                log.info("Begin loading documents of index index {}.", entry.getKey());
                Collections.sort(entry.getValue());
                indexName.setValue(entry.getKey());
                MutableLong counter = new MutableLong(0);
                MongoCollection<Document> collection = database.getCollection(indexName.getValue());
                for (PathAndOrder pathAndOrder : entry.getValue()) {
                    // Run on each zip of index
                    log.info("Processing zip file {}.", pathAndOrder.path);
                    loadZip(pathAndOrder.path, (entryName, doc) -> {
                        // Run on each doc
                        lastDocId.setValue(entryName);
                        List<Document> mongoDocs = convertEsDocToMongoDocs(doc);
                        try {
                            collection.insertMany(mongoDocs);
                        } catch (BsonMaximumSizeExceededException e) {
                            log.error("Too large doc '{}'", doc.get("id"), e);
                            tooLargeDocsDetected.setTrue();
                            writeTooLarge(indexName.getValue(), entryName, doc);
                        }
                        if (counter.incrementAndGet() % 200 == 0) {
                            log.info("Processed {} documents from index {}.", counter.longValue(), indexName.getValue());
                        }
                    });
                }
                log.info("Done - loaded {} docs of index '{}' to Mongo.", counter.longValue(), indexName.getValue());
            }
        } catch (RuntimeException e) {
            log.error("Error writing to Mongo. Index '{}'. Last doc: {}", indexName, lastDocId.getValue(), e);
            return false;
        }
        if (tooLargeDocsDetected.isTrue()) {
            log.warn("Too large docs encountered and saved to {}", tooLargeDir.toAbsolutePath());
        }
        log.info("Done loading all requested {} indices to Mongo.", sortedFilesByIndex.size());
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

    private List<Document> convertEsDocToMongoDocs(Map<String,Object> doc) {
        cleanDescriptions(doc);
        List<Document> bsonDocs = buildDocsFromSplit(Collections.singletonList(new Document(doc)), "fileSystemPaths", MAX_LARGE_COLLECTION_SIZE);
        bsonDocs = buildDocsFromSplit(bsonDocs, "applications", MAX_LARGE_COLLECTION_SIZE);
        bsonDocs = buildDocsFromSplit(bsonDocs, "incidentThreads", MAX_INCIDENT_THREADS);
        int docsCountBeforeFullLogSplit = bsonDocs.size();
        bsonDocs = buildDocsFromSplitTextField(bsonDocs, "fullLog", MAX_TEXT_FIELD_SIZE);
        boolean fullLogSplit = bsonDocs.size() > docsCountBeforeFullLogSplit;
        if (fullLogSplit) {
            bsonDocs.forEach(document -> document.remove("trust"));
        } else {
            bsonDocs = buildDocsFromSplitTextField(bsonDocs, "trust", MAX_TEXT_FIELD_SIZE);
        }
        return bsonDocs;
    }




    private static List<Document> buildDocsFromSplit(List<Document> docs, String collectionField, int maxSize) {
        return docs.stream().flatMap(doc -> buildDocsFromSplit(doc, collectionField, maxSize).stream()).collect(Collectors.toList());
    }

    private static List<Document> buildDocsFromSplit(Document bsonDoc, String collectionField, int maxSize) {
        List<Map<String,Object>> collection = (List<Map<String,Object>>)bsonDoc.get(collectionField);
        if (collection == null || collection.size() <= maxSize) {
            bsonDoc.putIfAbsent("_id", bsonDoc.get("id"));
            return List.of(bsonDoc);
        } else {
            List<Document> bsonDocs = new ArrayList<>(collection.size() / maxSize + 1);
            for (int i = 0, splitNum = 0; i < collection.size(); i += maxSize, splitNum++) {
                List<Map<String,Object>> batch = collection.subList(i, Math.min(collection.size(), i + maxSize));
                Document modDoc = new Document(bsonDoc);
                modDoc.put(collectionField, batch);
                String id = modDoc.computeIfAbsent("_id", k -> modDoc.get("id")) + "_" + splitNum;
                modDoc.put("id", id);
                modDoc.put("_id", id);
                bsonDocs.add(modDoc);
            }
            return bsonDocs;
        }
    }

    private static List<Document> buildDocsFromSplitTextField(List<Document> docs, String textField, int maxSize) {
        return docs.stream().flatMap(doc -> buildDocsFromSplitTextField(doc, textField, maxSize).stream()).collect(Collectors.toList());
    }

    private static List<Document> buildDocsFromSplitTextField(Document doc, String textField, int maxSize) {
        String text = (String)doc.get(textField);
        if (text == null || text.length() <= maxSize) {
            doc.putIfAbsent("_id", doc.get("id"));
            return List.of(doc);
        } else {
            List<Document> bsonDocs = new ArrayList<>(text.length() / maxSize + 1);
            for (int i = 0, splitNum = 0; i < text.length(); i += maxSize, splitNum++) {
                String batch = text.substring(i, Math.min(text.length(), i + maxSize));
                Document bsonDoc = new Document(doc);
                bsonDoc.put(textField, batch);
                String id = bsonDoc.computeIfAbsent("_id", k -> bsonDoc.get("id")) + "_" + splitNum;
                bsonDoc.put("id", id);
                bsonDoc.put("_id", id);
                bsonDocs.add(bsonDoc);
            }
            return bsonDocs;
        }
    }

    private static void cleanDescriptions(Map<String,Object> doc) {
        long size = 0;
        List<String> descriptions = (List<String>)doc.get("descriptions");
        if (descriptions != null) {
            for (String description : descriptions) {
                if (description != null) {
                    size += description.length();
                }
            }
        }
        List<Map<String,Object>> analysisRules = (List<Map<String,Object>>)doc.get("analysisRules");
        if (analysisRules != null) {
            for (Map<String,Object> analysisRule : analysisRules) {
                String description = (String)analysisRule.get("description");
                if (description != null) {
                    size += description.length();
                }
                String generalDescription = (String)analysisRule.get("generalDescription");
                if (generalDescription != null) {
                    size += generalDescription.length();
                }

                String extendedInfo = (String)analysisRule.get("extendedInfo");
                if (extendedInfo != null && extendedInfo.length() > MAX_EXTENDED_INFO_TEXT_SIZE) {
                    analysisRule.remove("extendedInfo");
                }
            }
        }
        if (size > MAX_DESCRIPTIONS_TEXT_SIZE) {
            doc.remove("descriptions");
            if (analysisRules != null) {
                for (Map<String,Object> analysisRule : analysisRules) {
                    analysisRule.remove("description");
                    analysisRule.remove("generalDescription");
                }
            }
        }
    }

    private void writeTooLarge(String index, String id, Map<String,Object> doc) {
        try {
            if (!Files.isDirectory(tooLargeDir)) {
                Files.createDirectories(tooLargeDir);
            }
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tooLargeDir.resolve(index + "_" + id + ".json").toFile(), doc);
        } catch (IOException ex) {
            log.error("Error writing large file to temp dir", ex);
        }
    }


    boolean loadStoreApplications() {
        if (mongoClient == null) {
            log.error("mongo client is not defined");
            return false;
        }
        MongoDatabase database = mongoClient.getDatabase(MONGO_DB_NAME);
        //  gluon_store_applications_00000.zip
        List<PathAndOrder> sortedFiles = new ArrayList<>();
        MutableObject<String> indexName = new MutableObject<>();
        try (Stream<Path> files = Files.list(exportedDataFolder)) {
            files.forEach(file -> {
                Matcher matcher = storeApplicationsFileNamePattern.matcher(file.getFileName().toString());
                if (matcher.matches()) {
                    indexName.setValue(matcher.group(1));
                    String number = matcher.group(2);
                    sortedFiles.add(new PathAndOrder(file, Integer.parseInt(number)));
                }
            });
        } catch (IOException e) {
            log.error("Error processing files list", e);
        }
        if (indexName.getValue() == null) {
            log.warn("No files found");
            return false;
        }
        Collections.sort(sortedFiles);
        MongoCollection<Document> collection = database.getCollection(indexName.getValue());
        MutableLong counter = new MutableLong();
        MutableLong tooLargeDocsDetected = new MutableLong();
        MutableObject<String> lastDocId = new MutableObject<>();
        try {
            for (PathAndOrder pathAndOrder : sortedFiles) {
                // Run on each zip of index
                log.info("Processing zip file {}.", pathAndOrder.path);
                loadLinesFromZip(pathAndOrder.path, (doc) -> {
                    Document mongoDoc = convertStoreAppsDoc(doc);
                    lastDocId.setValue((String)mongoDoc.get("_id"));
                    try {
                        collection.insertOne(mongoDoc);
                    } catch (BsonMaximumSizeExceededException e) {
                        log.error("Too large doc '{}'", doc.get("id"), e);
                        tooLargeDocsDetected.increment();
                    }
                    if (counter.incrementAndGet() % 500 == 0) {
                        log.info("Processed {} documents from index {}.", counter.longValue(), indexName.getValue());
                    }
                });
            }
            log.info("Done - loaded {} docs of index '{}' to Mongo.", counter.longValue(), indexName.getValue());
        } catch (RuntimeException e) {
            log.error("Error writing to Mongo. Index '{}'. Last doc: {}", indexName, lastDocId.getValue(), e);
            return false;
        }
        if (tooLargeDocsDetected.getValue() > 0) {
            log.warn("Too large docs encountered: {}", tooLargeDocsDetected.getValue());
        }
        log.info("Done loading application store index with {} docs to Mongo.", counter.longValue());
        return true;
    }


    private void loadLinesFromZip(Path file, Consumer<Map<String,Object>> docsConsumer) {
        try (ZipFile zipFile = new ZipFile(file.toFile())) {
            zipFile.stream().filter(e -> !e.isDirectory()).forEach(e -> {
                try (InputStream inputStream = zipFile.getInputStream(e);
                     BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        Map<String,Object> doc = objectMapper.readValue(line, mapStringObjectTypeRef);
                        docsConsumer.accept(doc);
                    }
                } catch (IOException exp) {
                    throw new ZipEntryRuntimeException("Error reading lines from entry " + e.getName(), exp);
                }
            });
        } catch (IOException e) {
            log.error("Error loading lines from zip file", e);
            throw new ZipReadingRuntimeException("Error reading zip " + file.toAbsolutePath(), e);
        }
    }

    private Document convertStoreAppsDoc(Map<String,Object> doc) {
        Document modDoc = new Document(doc);
        String id = modDoc.get("platform") + "_" + modDoc.get("bundleId");
        modDoc.put("_id", id);
        return modDoc;
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

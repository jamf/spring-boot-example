package com.zecops.example.mongo;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("mongoImport")
public class MongoAdapterController {

    private final MongoAdapterService mongoAdapterService;

    public MongoAdapterController(MongoAdapterService mongoAdapterService) {
        this.mongoAdapterService = mongoAdapterService;
    }

    @PostMapping("filesIndices")
    public ResponseEntity<?> importFilesIndices() {
        boolean success = mongoAdapterService.loadFilesIndices();
        if (success) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("metadataIndices")
    public ResponseEntity<?> importMetadataIndices() {
        boolean success = mongoAdapterService.loadMetadataIndices();
        if (success) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.internalServerError().build();
        }
    }

    @PostMapping("storeApplications")
    public ResponseEntity<?> importStoreApplications() {
        boolean success = mongoAdapterService.loadStoreApplications();
        if (success) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.internalServerError().build();
        }
    }

}

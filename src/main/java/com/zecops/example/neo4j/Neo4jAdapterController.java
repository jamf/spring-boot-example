package com.zecops.example.neo4j;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("neo4jImport")
public class Neo4jAdapterController {

    private final Neo4jAdapterService neo4jAdapterService;

    public Neo4jAdapterController(Neo4jAdapterService neo4jAdapterService) {
        this.neo4jAdapterService = neo4jAdapterService;
    }

    @PostMapping("loadDirlists")
    public ResponseEntity<?> importDirlists() {
        boolean success = neo4jAdapterService.loadDirlists();
        if (success) {
            return ResponseEntity.ok().build();
        } else {
            return ResponseEntity.internalServerError().build();
        }
    }

}

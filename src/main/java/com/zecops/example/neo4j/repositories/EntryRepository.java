package com.zecops.example.neo4j.repositories;

import com.zecops.example.neo4j.model.Entry;
import org.springframework.data.neo4j.repository.Neo4jRepository;

import java.util.UUID;

public interface EntryRepository extends Neo4jRepository<Entry, String> {

}

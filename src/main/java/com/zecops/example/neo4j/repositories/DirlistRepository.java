package com.zecops.example.neo4j.repositories;

import com.zecops.example.neo4j.model.Dirlist;
import org.springframework.data.neo4j.repository.Neo4jRepository;

public interface DirlistRepository extends Neo4jRepository<Dirlist, String> {


}

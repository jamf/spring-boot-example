# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A deliberately minimal Spring Boot service used for education and interviews. The entire application is two classes (`ExampleApplication`, `ExampleController`) exposing two endpoints. Keep changes small and self-contained unless asked otherwise.

- `GET /` — returns `{"message": "Hello world!"}`
- `POST /shutdown` — gracefully exits the JVM (spawns a thread that calls `SpringApplication.exit`)
- Swagger UI at `/swagger-ui.html` (springdoc)
- Server listens on port **1080** (`application.properties`)

## Commands

```bash
mvn package              # compile + build the snapshot jar into target/
mvn install              # also assembles the distributable into target/dist/ (see Deployment)
mvn clean install        # full rebuild
```

There is **no test suite** — no `src/test`, no test dependencies, and no `spring-boot-maven-plugin`. If you add tests, you must also add the test dependencies (e.g. `spring-boot-starter-test`) to `pom.xml` first. Run a single test with `mvn test -Dtest=ClassName#method` only after that infrastructure exists.

To run locally for manual checks, the simplest path is `mvn install` then the deployment run command below.

## Deployment model (non-obvious)

This project does **not** produce an executable fat jar. Instead:

1. `mvn install` uses `maven-dependency-plugin` to copy all compile-scope dependencies into `target/dist/lib/`, copies the app jar there too, and `maven-resources-plugin` copies `application.properties`, `logback-spring.xml`, and `README.md` into `target/dist/`.
2. Deploy by copying the contents of `target/dist/` to the server.
3. Start with: `java -cp 'lib/*' com.zecops.example.ExampleApplication` (run from the dist directory).

Logs are written to a `log/` folder relative to the working directory (`logback-spring.xml`, rolling daily/by-size, gzip).

## Gotchas

- **Package vs. owner mismatch**: the Java package and Maven `groupId` are `com.zecops`, but the repo is now owned by `jamf`/`crocodile` (CODEOWNERS, catalog-info.yaml). Renaming the package is a deliberate, separate effort — don't assume `com.zecops` is wrong.
- **`pom.xml` is the source of truth for versions** — currently Spring Boot 4.0.6 (Spring Framework 7, Jackson 3), Java 17. Any `target/` artifacts may show stale versions from an older build; ignore them.
- **Dependency versions come from the `spring-boot-dependencies` BOM** (imported in `<dependencyManagement>` with `<scope>import</scope>`), not the `spring-boot-starter-parent`. Don't add explicit `<version>` to anything the BOM manages (e.g. `spring-boot-starter-web`); only third-party deps the BOM doesn't cover need a version. `springdoc-openapi-starter-webmvc-ui` is one such — it's pinned to `3.0.3` because Spring Boot 4 requires springdoc 3.x (the old 2.x line is Boot 3 only).
- The old explicit `snakeyaml` 2.2 override was removed in the Boot 4 upgrade — the BOM now manages a safe version (2.5), so let it.

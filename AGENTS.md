# Repository Guidelines

## Project Structure & Module Organization
This repository is a single Maven OSGi bundle (`org.apache.sling.event`).

- `src/main/java/org/apache/sling/event/impl/...`: production code for job handling, queueing, scheduling, stats, and topology integration.
- `src/main/resources/SLING-INF/...`: bundle resources (for example JCR nodetype definitions).
- `src/test/java/...`: tests, with unit tests in `impl/**` and integration-style tests in `it/**`.
- `src/test/resources/`: test logging/config resources.
- Root build/config files: `pom.xml`, `bnd.bnd`, `Jenkinsfile`, `.asf.yaml`.

## Build, Test, and Development Commands
- `mvn clean verify`: full build, unit tests (Surefire), and integration tests (Failsafe/Pax Exam).
- `mvn test`: run unit tests only for fast feedback.
- `mvn -Dtest=JobManagerImplTest test`: run one unit test class.
- `mvn -Dit.test=JobHandlingIT verify`: run one integration test class.
- `mvn -DskipTests package`: build the bundle quickly into `target/`.
- `mvn spotless:apply`: run after every change to enforce repository formatting.

## Coding Style & Naming Conventions
- Java style follows existing Sling code: 4-space indentation, same-line braces, and clear Javadoc on public behavior.
- Keep the ASF license header at the top of new Java files.
- Package names stay under `org.apache.sling.event.impl`.
- Naming: classes `PascalCase`, methods/fields `camelCase`, constants `UPPER_SNAKE_CASE`.
- Prefer SLF4J parameterized logging (`logger.debug("Job {}", id)`), not string concatenation.

## Testing Guidelines
- Primary frameworks: JUnit 4, Mockito, Sling Mocks, and Pax Exam for integration tests.
- Name unit tests `*Test.java`; name integration tests `*IT.java` so Maven plugins pick them up correctly.
- Every bug fix or behavior change should include a focused test.
- Avoid hardcoded ports in ITs; the build reserves ports during `pre-integration-test`.

## Commit & Pull Request Guidelines
- Follow commit subjects used in history: `SLING-<issue> concise imperative summary` (example: `SLING-13044 reschedule jobs via dedicated threadpool`).
- Keep commits narrowly scoped; separate refactors from behavior changes.
- PRs should include: linked issue, what changed, why, and the exact test command(s) run.
- Target the `master` branch and call out any operational/configuration impact explicitly.

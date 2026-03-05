# CI Setup Guide — Branch Protection & GHCR

## 1. Required GitHub Repository Settings

### Branch Protection for `main`

Navigate to **Settings → Branches → Add rule** for `main` and enable:

| Setting | Value |
|---------|-------|
| Require status checks to pass before merging | ✅ Enabled |
| Require branches to be up to date before merging | ✅ Enabled |
| Required status checks | `Build & Unit Tests`, `Integration Tests (Testcontainers)` |
| Require pull request reviews before merging | ✅ Recommended (1 reviewer minimum) |
| Restrict who can push to matching branches | ✅ Restrict to platform team |
| Do not allow bypassing the above settings | ✅ Enabled |

> ⚠️ **Important:** The Docker Build & Push job is NOT required as a status check — it only runs after merge to `main`.
> The two jobs that block PRs are `Build & Unit Tests` and `Integration Tests (Testcontainers)`.

---

## 2. GitHub Container Registry (GHCR) Setup

Images are pushed to `ghcr.io/<your-org>/broker-proxy` and `ghcr.io/<your-org>/koserver`.

### Automatic setup (no secrets needed)
- The CI uses `GITHUB_TOKEN` which is automatically provided by GitHub Actions.
- Packages are created under your GitHub organization on first push to `main`.

### Making packages public (optional)
Navigate to **GitHub → Packages → broker-proxy → Package settings → Change visibility → Public**.

### Pulling images locally
```bash
# Log in
echo $GITHUB_TOKEN | docker login ghcr.io -u YOUR_USERNAME --password-stdin

# Pull latest
docker pull ghcr.io/<your-org>/broker-proxy:latest
docker pull ghcr.io/<your-org>/koserver:latest

# Pull specific SHA
docker pull ghcr.io/<your-org>/broker-proxy:sha-a1b2c3d
```

---

## 3. Maven Profile Convention (required in pom.xml)

The CI pipeline expects these Maven conventions:

### Unit tests (Job 1)
```bash
# Runs Surefire only; skips Failsafe
./mvnw verify -DskipITs -Dskip.failsafe=true
```
- Test class naming: `*Test.java`, `Test*.java`

### Integration tests (Job 2)
```bash
# Activates the `integration-tests` profile, skips Surefire
./mvnw verify -Pintegration-tests -DskipUnitTests=true
```
- IT class naming: `*IT.java`, `*ITCase.java`, `IT*.java`

### Required pom.xml additions

**Root `pom.xml`** must include the `integration-tests` profile:

```xml
<profiles>
  <profile>
    <id>integration-tests</id>
    <build>
      <plugins>
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-failsafe-plugin</artifactId>
          <version>3.2.5</version>
          <executions>
            <execution>
              <goals>
                <goal>integration-test</goal>
                <goal>verify</goal>
              </goals>
            </execution>
          </executions>
        </plugin>
        <!-- Skip Surefire in IT profile -->
        <plugin>
          <groupId>org.apache.maven.plugins</groupId>
          <artifactId>maven-surefire-plugin</artifactId>
          <configuration>
            <skip>${skipUnitTests}</skip>
          </configuration>
        </plugin>
      </plugins>
    </build>
  </profile>
</profiles>
```

---

## 4. CI Pipeline Diagram

```
Push / PR
    │
    ▼
┌─────────────────────────────────┐
│  Job 1: Build & Unit Tests      │  ← Blocks PR merge
│  ./mvnw verify -DskipITs        │
│  Uploads: surefire-reports/     │
└───────────────┬─────────────────┘
                │ needs: job 1
                ▼
┌─────────────────────────────────┐
│  Job 2: Integration Tests       │  ← Blocks PR merge
│  ./mvnw verify -Pintegration    │
│  Docker: redis, zk, activemq    │
│  Uploads: failsafe-reports/     │
└───────────────┬─────────────────┘
                │ needs: job 1+2
                │ only on: push to main
                ▼
┌─────────────────────────────────┐
│  Job 3: Docker Build & Push     │  ← main only
│  ghcr.io/.../broker-proxy:sha-* │
│  ghcr.io/.../koserver:sha-*     │
└─────────────────────────────────┘
```

---

## 5. Local CI Simulation

Run the full pipeline locally before pushing:

```bash
# Unit tests
./mvnw verify -DskipITs -Dskip.failsafe=true

# Integration tests (requires Docker running)
./mvnw verify -Pintegration-tests -DskipUnitTests=true

# Docker build only (no push)
docker build -f docker/broker-proxy/Dockerfile -t broker-proxy:local .
docker build -f docker/koserver/Dockerfile     -t koserver:local .
```

---

## 6. Testcontainers Resources

Pre-pull images before running ITs for faster local execution:

```bash
docker pull redis:7.2-alpine
docker pull confluentinc/cp-zookeeper:7.6.1
docker pull apache/activemq-classic:5.18.4
```

### Recommended Testcontainers dependencies (`pom.xml`)

```xml
<dependency>
  <groupId>org.testcontainers</groupId>
  <artifactId>testcontainers</artifactId>
  <version>1.19.7</version>
  <scope>test</scope>
</dependency>
<dependency>
  <groupId>org.testcontainers</groupId>
  <artifactId>junit-jupiter</artifactId>
  <version>1.19.7</version>
  <scope>test</scope>
</dependency>
<!-- Generic container for Redis, ZooKeeper, ActiveMQ -->
<dependency>
  <groupId>org.testcontainers</groupId>
  <artifactId>toxiproxy</artifactId>  <!-- optional: for Sentinel failover tests -->
  <version>1.19.7</version>
  <scope>test</scope>
</dependency>
```

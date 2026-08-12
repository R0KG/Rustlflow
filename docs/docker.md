# Docker Compose

Docker Compose starts the full local system:

~~~bash
docker compose -f deploy/compose.yaml up -d --build --scale worker=3
~~~

| Service | Address | Purpose |
| --- | --- | --- |
| API | http://localhost:3000 | HTTP API |
| PostgreSQL | localhost:5433 | Metadata and task state |
| NATS | localhost:4222 | JetStream task delivery |
| NATS monitoring | http://localhost:8222 | NATS monitoring endpoint |
| MinIO API | http://localhost:9000 | Object storage |
| MinIO console | http://localhost:9001 | Object storage UI |
| Jaeger | http://localhost:16686 | Trace UI |

The first PostgreSQL startup applies the SQL files in migrations/. The
minio-init service creates the RustFlow bucket before the API and workers
start.

The API and workers use MinIO by default. To run outside Compose with local
filesystem storage, set RUSTFLOW_STORAGE_BACKEND=local. Use
RUSTFLOW_LOCAL_STORAGE_ROOT to choose the local object directory.

Stop services while keeping local Compose volumes:

~~~bash
docker compose -f deploy/compose.yaml down
~~~

Delete the local database, JetStream data, MinIO data, and shared volumes:

~~~bash
docker compose -f deploy/compose.yaml down -v
~~~

The second command permanently deletes local development data.

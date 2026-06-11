# ai-classifier-service — Especificación técnica

## Propósito

Servicio encargado de clasificar automáticamente las incidencias urbanas reportadas por ciudadanos. Actúa de forma completamente asíncrona: consume el evento `incident.reported` de Kafka, ejecuta un pipeline de clasificación basado en RAG con Spring AI, y publica el resultado como evento `incident.classified`. Adicionalmente notifica al `incident-service` vía HTTP para que actualice los metadatos de clasificación de la incidencia.

---

## Responsabilidades

- Escuchar el topic Kafka `incident.reported`
- Clasificar el **tipo específico** de incidencia a partir de su descripción y metadatos
- Determinar el **nivel de urgencia** (BAJA, MEDIA, ALTA, CRÍTICA)
- Usar RAG para enriquecer la clasificación con contexto de incidencias previas y normativa municipal
- Publicar el evento `incident.classified` con el resultado de la clasificación
- Actualizar la incidencia en `incident-service` vía HTTP con los datos de clasificación
- Gestionar los errores de clasificación y publicar en un topic de dead-letter si el proceso falla

---

## Arquitectura hexagonal

```
ai-classifier-service/
└── src/main/java/com/citypulse/classifier/
    ├── domain/
    │   ├── model/
    │   │   ├── Incident.java               # representación interna de la incidencia
    │   │   ├── Classification.java         # resultado de clasificación (tipo + urgencia)
    │   │   ├── IncidentType.java           # enum de tipos de incidencia
    │   │   └── UrgencyLevel.java           # enum de niveles de urgencia
    │   ├── port/
    │   │   ├── in/
    │   │   │   └── ClassifyIncidentUseCase.java   # interfaz del caso de uso principal
    │   │   └── out/
    │   │       ├── KnowledgeBasePort.java         # búsqueda de contexto RAG
    │   │       ├── ClassificationEventPort.java   # publicación de evento clasificado
    │   │       └── IncidentUpdatePort.java        # notificación a incident-service
    │   └── service/
    │       └── ClassificationService.java         # lógica de negocio pura
    ├── application/
    │   └── usecase/
    │       └── ClassifyIncidentUseCaseImpl.java   # orquesta el pipeline completo
    └── infrastructure/
        ├── messaging/
        │   ├── IncidentReportedConsumer.java      # adaptador in: consumer Kafka
        │   ├── IncidentClassifiedProducer.java    # adaptador out: producer Kafka
        │   └── DeadLetterProducer.java            # adaptador out: errores
        ├── ai/
        │   ├── SpringAiClassificationAdapter.java # adaptador out: llama al LLM
        │   ├── PgVectorKnowledgeBaseAdapter.java  # adaptador out: búsqueda vectorial
        │   └── prompt/
        │       └── ClassificationPromptBuilder.java # construcción del prompt RAG
        ├── http/
        │   └── IncidentServiceHttpAdapter.java    # adaptador out: HTTP a incident-service
        └── config/
            ├── KafkaConfig.java
            ├── SpringAiConfig.java
            └── VectorStoreConfig.java
```

---

## Modelo de dominio

### `IncidentType` (enum)

Representa el tipo específico de incidencia. El clasificador debe ser capaz de asignar uno de estos valores o `UNKNOWN` si la descripción no es suficientemente clara.

```
BACHE
PAVIMENTO_DETERIORADO
SEÑALIZACION_DAÑADA
FAROLA_FUNDIDA
FAROLA_AVERIADA
SEMAFORO_DEFECTUOSO
RED_ELECTRICA
GRAFITI
RESIDUOS_VOLUMINOSOS
LIMPIEZA_VIARIA
OTRO
UNKNOWN
```

### `UrgencyLevel` (enum)

```
BAJA      // incidencia estética o de bajo impacto, puede esperar días
MEDIA     // afecta al servicio normal, atención en 48h
ALTA      // riesgo de accidente o afectación severa, atención en 24h
CRÍTICA   // peligro inmediato para personas o infraestructuras, escalado inmediato
```

### `Incident` (model)

Representación interna de la incidencia recibida desde el evento Kafka. Solo contiene los campos necesarios para clasificar; no replica toda la entidad del `incident-service`.

Campos relevantes para la clasificación:
- `incidentId` (UUID)
- `correlationId` (UUID) — para trazabilidad cross-service
- `rawDescription` (String) — texto libre del ciudadano
- `location` (coordenadas) — puede influir en la urgencia (zona escolar, hospital...)
- `reportedAt` (Instant)

### `Classification` (model)

Resultado del proceso de clasificación. Es el objeto que se persiste y se publica como evento.

Campos:
- `incidentId` (UUID)
- `correlationId` (UUID)
- `incidentType` (IncidentType)
- `urgencyLevel` (UrgencyLevel)
- `confidenceScore` (double, 0.0–1.0) — confianza del LLM en la clasificación
- `ragContextSummary` (String) — resumen del contexto recuperado del vector store
- `classifiedAt` (Instant)
- `modelUsed` (String) — nombre del modelo LLM empleado

---

## Casos de uso

### `ClassifyIncidentUseCase`

Único caso de uso del servicio. Orquesta todo el pipeline de clasificación.

**Flujo:**

1. Recibe un `Incident` desde el consumer Kafka
2. Delega en `ClassificationService` la lógica de negocio:
   - Recupera contexto relevante del vector store (`KnowledgeBasePort`)
   - Construye el prompt con descripción + contexto RAG
   - Llama al LLM y parsea la respuesta como `Classification`
   - Valida que el resultado sea coherente (tipo no nulo, score mínimo aceptable)
3. Si la clasificación es válida:
   - Publica `incident.classified` (`ClassificationEventPort`)
   - Notifica a `incident-service` con el resultado (`IncidentUpdatePort`)
4. Si la clasificación falla o el score es demasiado bajo:
   - Asigna tipo `UNKNOWN` y urgencia `MEDIA` por defecto
   - Publica igualmente el evento para no bloquear el pipeline
   - Registra el fallo en logs con el `correlationId`
5. Si hay un error irrecuperable (timeout LLM, excepción no controlada):
   - Publica el mensaje original en el topic dead-letter
   - No reintenta automáticamente (Kafka se encarga del retry con back-off)

---

## Puertos de salida

### `KnowledgeBasePort`

Abstrae la búsqueda de contexto en el vector store.

```java
public interface KnowledgeBasePort {
    List<KnowledgeDocument> findSimilarIncidents(String description, int topK);
    List<KnowledgeDocument> findRelevantRegulations(String description, int topK);
}
```

La implementación (`PgVectorKnowledgeBaseAdapter`) usa el `VectorStore` de Spring AI para buscar los `topK` documentos más similares semánticamente a la descripción de la incidencia. Se hacen dos búsquedas separadas: una sobre la colección de incidencias históricas y otra sobre la colección de normativa municipal.

### `ClassificationEventPort`

```java
public interface ClassificationEventPort {
    void publishClassified(Classification classification);
    void publishDeadLetter(String rawEventPayload, String reason);
}
```

### `IncidentUpdatePort`

```java
public interface IncidentUpdatePort {
    void updateClassification(UUID incidentId, Classification classification);
}
```

---

## Pipeline RAG

### Base de conocimiento (vector store)

Se mantienen dos colecciones de documentos en pgvector:

**Colección `incident-history`:** incidencias previas resueltas, con su tipo real y nivel de urgencia. Sirven como ejemplos few-shot contextualizados. Cada documento incluye: descripción original, tipo asignado, urgencia, zona y resolución.

**Colección `regulations`:** fragmentos de normativa municipal sobre tiempos de respuesta, definición de urgencias, y competencias por departamento. Permiten al LLM razonar sobre urgencia con criterio normativo.

### Ingesta de documentos

La ingesta inicial de la base de conocimiento se realiza mediante un componente separado (`KnowledgeBaseLoader`) que se ejecuta al arrancar el servicio si el vector store está vacío. Lee documentos de ficheros JSON/texto bajo `src/main/resources/knowledge/` y los vectoriza con el modelo de embeddings configurado.

En producción, la ingesta de nuevas incidencias resueltas puede hacerse de forma incremental consumiendo el evento `incident.resolved` de Kafka y vectorizando la incidencia resuelta para enriquecer la base de conocimiento.

### Construcción del prompt

El `ClassificationPromptBuilder` construye un prompt estructurado con:

1. **System prompt:** rol del asistente (experto en gestión de incidencias urbanas municipales), instrucciones de formato de salida, lista de tipos válidos, criterios de urgencia según normativa.
2. **Contexto RAG:** los documentos recuperados del vector store, formateados como ejemplos o referencias normativas.
3. **User prompt:** la descripción de la incidencia a clasificar, con ubicación si está disponible.

La salida esperada del LLM es un JSON estructurado con `incidentType`, `urgencyLevel` y `confidenceScore`. Spring AI se encarga del binding a la clase `Classification` mediante `BeanOutputConverter`.

---

## Adaptadores de infraestructura

### `IncidentReportedConsumer` (Kafka consumer)

- Topic: `incident.reported`
- Consumer group: `ai-classifier-service`
- Deserialización: JSON → `IncidentReportedEvent` (DTO de entrada)
- Mapea el DTO al modelo de dominio `Incident` antes de invocar el caso de uso
- Configuración de errores: `DefaultErrorHandler` con back-off exponencial (3 reintentos, delay inicial 1s, multiplicador 2)
- Tras agotar reintentos: publica en dead-letter topic

### `IncidentClassifiedProducer` (Kafka producer)

- Topic: `incident.classified`
- Serialización: `Classification` → JSON
- Incluye `correlationId` como header Kafka para trazabilidad

### `IncidentServiceHttpAdapter` (HTTP client)

- Usa `WebClient` (Spring WebFlux) o `RestClient` (Spring 6) para llamada no bloqueante
- Endpoint destino: `PATCH /api/v1/incidents/{incidentId}/classification`
- Timeout configurado en `application.yml`
- En caso de fallo HTTP (4xx, 5xx, timeout): loguea el error pero no interrumpe el flujo; la clasificación ya fue publicada en Kafka
- La actualización HTTP es best-effort; `incident-service` puede reconstruir el estado desde los eventos Kafka

### `PgVectorKnowledgeBaseAdapter` (vector store)

- Usa `PgVectorStore` de Spring AI
- Modelo de embeddings: configurable via `application.yml` (por defecto: `text-embedding-ada-002` de OpenAI o modelo local compatible)
- `topK` configurable (por defecto: 5 documentos por búsqueda)
- Distancia: coseno

---

## Configuración (`application.yml`)

```yaml
spring:
  application:
    name: ai-classifier-service

  kafka:
    bootstrap-servers: ${KAFKA_BOOTSTRAP_SERVERS:localhost:9092}
    consumer:
      group-id: ai-classifier-service
      auto-offset-reset: earliest
    producer:
      key-serializer: org.apache.kafka.common.serialization.StringSerializer

  datasource:
    url: ${PGVECTOR_URL:jdbc:postgresql://localhost:5432/classifier_db}
    username: ${PGVECTOR_USER:citypulse}
    password: ${PGVECTOR_PASSWORD}

  ai:
    openai:
      api-key: ${OPENAI_API_KEY}
      chat:
        options:
          model: gpt-4o-mini
          temperature: 0.1       # baja temperatura para clasificaciones deterministas
      embedding:
        options:
          model: text-embedding-ada-002
    vectorstore:
      pgvector:
        index-type: HNSW
        distance-type: COSINE_DISTANCE
        dimensions: 1536

citypulse:
  classifier:
    rag:
      top-k-incidents: 5
      top-k-regulations: 3
      min-confidence-score: 0.6   # por debajo → UNKNOWN
    incident-service:
      base-url: ${INCIDENT_SERVICE_URL:http://incident-service:8080}
      timeout-seconds: 5
  kafka:
    topics:
      incident-reported: incident.reported
      incident-classified: incident.classified
      dead-letter: incident.classifier.dead-letter
```

---

## Persistencia

La única base de datos propia de este servicio es el vector store en pgvector. No tiene base de datos relacional propia.

**Schema pgvector:**

- Tabla `vector_store` (gestionada por Spring AI): almacena los embeddings junto con metadata en formato JSONB. La metadata incluye el tipo de colección (`incident-history` o `regulations`), el identificador del documento fuente y campos de filtrado.

**Inicialización:**

Spring AI puede crear automáticamente la tabla `vector_store` con `spring.ai.vectorstore.pgvector.initialize-schema=true`. Activar solo en entornos de desarrollo; en producción gestionar con Flyway o Liquibase.

---

## Kafka topics

| Topic | Rol | Descripción |
|---|---|---|
| `incident.reported` | Consumidor | Evento de nueva incidencia publicado por `incident-service` |
| `incident.classified` | Productor | Evento con el resultado de la clasificación IA |
| `incident.classifier.dead-letter` | Productor | Mensajes que no pudieron procesarse tras reintentos |

---

## Tests

### Tests unitarios (`src/test/java/.../unit/`)

Sin Spring context. Prueban la lógica de dominio de forma aislada con mocks de los puertos de salida.

- `ClassificationServiceTest`: verifica que dado un `Incident` y un contexto RAG simulado, el servicio produce la `Classification` correcta. Cubre casos: clasificación exitosa, score bajo → UNKNOWN, tipo de incidencia desconocido.
- `ClassificationPromptBuilderTest`: verifica que el prompt generado incluye el contexto RAG, la descripción de la incidencia y las instrucciones de formato correctas.

### Tests de integración (`src/test/java/.../integration/`)

Con Testcontainers. Levantan infraestructura real en Docker.

- `ClassifyIncidentUseCaseIntegrationTest`: arranca Kafka + pgvector con Testcontainers. Publica un evento en `incident.reported`, espera que el consumer lo procese, y verifica que se publica en `incident.classified` con el tipo y urgencia esperados. Usa WireMock para simular `incident-service`.
- `PgVectorKnowledgeBaseAdapterTest`: verifica que los documentos se guardan y se recuperan correctamente por similitud semántica desde un contenedor pgvector real.
- `IncidentReportedConsumerRetryTest`: verifica que ante un fallo del LLM (simulado con WireMock devolviendo 500), el consumer reintenta el número de veces configurado y finalmente publica en dead-letter.

---

## Dependencias Maven (`pom.xml`)

```xml
<!-- Spring Boot -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-web</artifactId>
</dependency>

<!-- Spring AI: core + OpenAI + pgvector -->
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-openai-spring-boot-starter</artifactId>
</dependency>
<dependency>
    <groupId>org.springframework.ai</groupId>
    <artifactId>spring-ai-pgvector-store-spring-boot-starter</artifactId>
</dependency>

<!-- Kafka -->
<dependency>
    <groupId>org.springframework.kafka</groupId>
    <artifactId>spring-kafka</artifactId>
</dependency>

<!-- PostgreSQL driver (para pgvector) -->
<dependency>
    <groupId>org.postgresql</groupId>
    <artifactId>postgresql</artifactId>
    <scope>runtime</scope>
</dependency>

<!-- HTTP client para incident-service -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-webflux</artifactId>
</dependency>

<!-- Observabilidad -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>

<!-- Tests -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-testcontainers</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>postgresql</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>com.github.tomakehurst</groupId>
    <artifactId>wiremock-jre8</artifactId>
    <scope>test</scope>
</dependency>
```

---

## Dockerfile

```dockerfile
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY target/ai-classifier-service-*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

Build multistage recomendado para producción (separar fase de compilación de la imagen final).

---

## Variables de entorno requeridas

| Variable | Descripción | Ejemplo |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | Dirección del broker Kafka | `kafka:9092` |
| `PGVECTOR_URL` | JDBC URL del vector store | `jdbc:postgresql://postgres:5432/classifier_db` |
| `PGVECTOR_USER` | Usuario PostgreSQL | `citypulse` |
| `PGVECTOR_PASSWORD` | Contraseña PostgreSQL | — |
| `OPENAI_API_KEY` | API key de OpenAI | — |
| `INCIDENT_SERVICE_URL` | URL base del incident-service | `http://incident-service:8080` |

---

## Observabilidad

- **Health check:** `GET /actuator/health` — incluye estado del vector store y la conectividad Kafka
- **Métricas Prometheus:** `GET /actuator/prometheus`
- **Métricas de negocio a exponer:**
  - `classifier.incidents.processed.total` — contador de incidencias procesadas
  - `classifier.incidents.unknown.total` — contador de clasificaciones UNKNOWN
  - `classifier.incidents.dead_letter.total` — contador de mensajes en dead-letter
  - `classifier.rag.duration.seconds` — histograma de tiempo de búsqueda RAG
  - `classifier.llm.duration.seconds` — histograma de tiempo de llamada al LLM

---

## Consideraciones de diseño específicas

**Temperatura LLM baja (0.1):** la clasificación debe ser determinista y reproducible. Una temperatura alta introduciría variabilidad innecesaria en un proceso que debería ser estable para las mismas entradas.

**Clasificación best-effort:** si el LLM no puede clasificar con suficiente confianza, el servicio asigna `UNKNOWN`/`MEDIA` y deja pasar el evento. El `routing-service` tiene lógica de fallback para incidencias `UNKNOWN` (las asigna a un operador de guardia). Nunca se bloquea el pipeline por una clasificación fallida.

**Ingesta incremental de conocimiento:** el vector store se enriquece con cada incidencia resuelta, mejorando la calidad de la clasificación con el tiempo. Esto convierte la base de conocimiento en un sistema que aprende del histórico operativo real del municipio.

**Sin estado propio de incidencias:** este servicio no almacena el estado de las incidencias. Su única fuente de verdad es el evento recibido y su única persistencia es el vector store. Esto lo hace stateless y fácilmente escalable horizontalmente.

**Aislamiento del LLM en la capa de infraestructura:** el dominio no conoce OpenAI, ni Spring AI, ni ningún modelo concreto. El puerto `KnowledgeBasePort` y la implementación en `ClassificationService` trabajan con abstracciones puras. Cambiar de OpenAI a un modelo local (Ollama, por ejemplo) solo requiere cambiar el adaptador de infraestructura y la configuración.

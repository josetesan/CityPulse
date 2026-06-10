# CityPulse — Definición de proyecto

## Descripción general

CityPulse es una plataforma de gestión de incidencias urbanas que permite a los ciudadanos reportar problemas en la ciudad (baches, farolas fundidas, grafitis, residuos...) y recibir notificaciones del estado y resolución de sus reportes. Los departamentos municipales gestionan las incidencias desde su propio backoffice y notifican la resolución a través de la plataforma.

El proyecto sirve como portfolio técnico que demuestra el uso de Spring Boot, Spring AI, arquitectura hexagonal, comunicación asíncrona con Apache Kafka, despliegue en Kubernetes y buenas prácticas de ingeniería de software.

---

## Objetivos técnicos

- Arquitectura de microservicios con responsabilidades claramente separadas
- Arquitectura hexagonal (ports & adapters) en cada microservicio
- Integración de Spring AI con RAG y agentes de IA
- Comunicación asíncrona entre servicios mediante Apache Kafka
- Persistencia con PostgreSQL, PostGIS y pgvector
- Notificación multicanal al ciudadano (email o SMS según preferencia)
- Despliegue en Kubernetes con manifests y pipelines CI/CD

---

## Microservicios

### 1. `incident-service`

**Responsabilidad:** punto de entrada al sistema. Recibe los reportes de incidencias del ciudadano a través de la API pública, los valida y los persiste.

**Casos de uso:**
- Crear una nueva incidencia (tipo, descripción, ubicación geográfica, foto opcional)
- Consultar el estado de una incidencia por su identificador
- Listar las incidencias de un ciudadano
- Actualizar el estado de una incidencia cuando llega un evento externo

**Puertos de entrada:**
- API REST para el ciudadano (crear, consultar incidencia)
- Consumidor Kafka para actualizar estado cuando otros servicios emiten eventos

**Puertos de salida:**
- Repositorio de incidencias (PostgreSQL + PostGIS)
- Productor Kafka: publica evento de nueva incidencia

**Persistencia:**
- Tabla de incidencias (identificador, tipo, descripción, ubicación, estado, ciudadano, timestamps)
- Tabla de historial de estados por incidencia

---

### 2. `user-service`

**Responsabilidad:** gestión del ciclo de vida de los ciudadanos registrados. Almacena el perfil y la preferencia de canal de notificación (email o teléfono).

**Casos de uso:**
- Registrar un ciudadano (nombre, email, teléfono, canal preferido)
- Consultar el perfil de un ciudadano
- Actualizar preferencias de notificación
- Autenticación básica (JWT)

**Puertos de entrada:**
- API REST pública

**Puertos de salida:**
- Repositorio de usuarios (PostgreSQL)

**Persistencia:**
- Tabla de usuarios (identificador, nombre, email, teléfono, canal preferido, timestamps)

---

### 3. `ai-classifier-service`

**Responsabilidad:** clasificación automática de incidencias usando Spring AI. Consume el evento de nueva incidencia, determina el tipo concreto y el nivel de urgencia usando RAG sobre incidencias anteriores y normativa municipal.

**Casos de uso:**
- Clasificar el tipo de incidencia (bache, farola, grafiti, residuos, etc.)
- Determinar el nivel de urgencia (baja, media, alta, crítica)
- Enriquecer la incidencia con metadatos de clasificación

**Puertos de entrada:**
- Consumidor Kafka: escucha evento de nueva incidencia

**Puertos de salida:**
- Productor Kafka: publica evento de incidencia clasificada
- Vector store (pgvector): lectura de embeddings para RAG
- Cliente HTTP al `incident-service` para actualizar la clasificación

**Componentes de IA:**
- RAG sobre base de conocimiento de incidencias anteriores y normativa municipal
- Embeddings almacenados en pgvector
- Prompt estructurado con salida de clasificación tipada

**Persistencia:**
- Base de conocimiento vectorial en pgvector (incidencias de referencia, normativa)

---

### 4. `routing-service`

**Responsabilidad:** decide a qué departamento municipal debe asignarse cada incidencia clasificada y publica el evento de enrutamiento.

**Casos de uso:**
- Mapear tipo de incidencia → departamento responsable
- Gestionar reglas de enrutamiento (configurables)
- Escalar a urgencias si el nivel de urgencia es crítico

**Puertos de entrada:**
- Consumidor Kafka: escucha evento de incidencia clasificada

**Puertos de salida:**
- Productor Kafka: publica evento de incidencia enrutada al departamento correspondiente
- Productor Kafka: publica evento de alerta de escalado si la urgencia es crítica

**Persistencia:**
- Tabla de reglas de enrutamiento (tipo de incidencia → departamento)

---

### 5. `department-service`

**Responsabilidad:** backoffice para los tres departamentos municipales (Vías y Obras, Alumbrado, Limpieza Urbana). Cada departamento consume únicamente las incidencias de su competencia y puede actualizar el estado de resolución.

**Departamentos gestionados:**
- **Vías y Obras:** baches, pavimento, señalización
- **Alumbrado:** farolas, red eléctrica, semáforos
- **Limpieza Urbana:** grafitis, residuos, limpieza viaria

**Casos de uso:**
- Listar incidencias pendientes del departamento
- Asignar una incidencia a un operario
- Marcar una incidencia como en progreso
- Marcar una incidencia como resuelta
- Añadir notas o evidencias de resolución

**Puertos de entrada:**
- API REST interna (backoffice de cada departamento)
- Consumidor Kafka: escucha evento de incidencia enrutada al departamento correspondiente

**Puertos de salida:**
- Productor Kafka: publica evento de incidencia resuelta
- Productor Kafka: publica evento de incidencia en progreso
- Repositorio de incidencias asignadas (PostgreSQL)

**Persistencia:**
- Tabla de asignaciones (incidencia, departamento, operario, estado, timestamps)
- Tabla de operarios por departamento

---

### 6. `notification-service`

**Responsabilidad:** escucha los eventos de resolución y cambio de estado de incidencias y notifica al ciudadano por el canal que eligió al registrarse (email o SMS).

**Casos de uso:**
- Notificar al ciudadano que su incidencia ha sido recibida
- Notificar al ciudadano que su incidencia está en progreso
- Notificar al ciudadano que su incidencia ha sido resuelta
- Seleccionar el canal de envío según la preferencia del ciudadano
- Registrar el historial de notificaciones enviadas

**Puertos de entrada:**
- Consumidor Kafka: escucha eventos de nueva incidencia, en progreso y resuelta

**Puertos de salida:**
- Cliente HTTP al `user-service` para obtener las preferencias del ciudadano
- Adaptador de email (SMTP o SendGrid)
- Adaptador de SMS (Twilio o AWS SNS)
- Repositorio de historial de notificaciones (PostgreSQL)

**Persistencia:**
- Tabla de notificaciones enviadas (canal, estado, destinatario, timestamps)

---

### 7. `analytics-service`

**Responsabilidad:** consume eventos del sistema para detectar patrones geoespaciales y temporales. Genera informes ejecutivos bajo demanda usando un agente de Spring AI.

**Casos de uso:**
- Detectar zonas con alta concentración de incidencias (PostGIS)
- Calcular tiempos medios de resolución por departamento y zona
- Generar informe ejecutivo semanal o mensual en PDF
- Consultar estadísticas por barrio, tipo de incidencia o departamento

**Puertos de entrada:**
- Consumidor Kafka: escucha todos los eventos del sistema para acumular métricas
- API REST interna para solicitar informes bajo demanda

**Puertos de salida:**
- Productor de informes PDF (mediante agente de Spring AI)
- Repositorio de métricas agregadas (PostgreSQL + PostGIS)

**Componentes de IA:**
- Agente redactor: recibe métricas estructuradas y genera texto ejecutivo
- Herramientas del agente: consulta de métricas, consulta geoespacial, generación de resumen

**Persistencia:**
- Tabla de métricas agregadas (tipo, zona, departamento, contadores, tiempos)

---

## Topics de Kafka

### Bus de entrada (ciclo de vida de la incidencia)

| Topic | Productor | Consumidores |
|---|---|---|
| `incident.reported` | `incident-service` | `ai-classifier-service`, `notification-service`, `analytics-service` |
| `incident.classified` | `ai-classifier-service` | `routing-service` |
| `incident.routed` | `routing-service` | `department-service`, `analytics-service` |
| `incident.escalated` | `routing-service` | `department-service`, `notification-service` |

### Bus de resolución

| Topic | Productor | Consumidores |
|---|---|---|
| `incident.in-progress` | `department-service` | `incident-service`, `notification-service`, `analytics-service` |
| `incident.resolved` | `department-service` | `incident-service`, `notification-service`, `analytics-service` |

---

## Infraestructura de datos

### PostgreSQL + PostGIS
Base de datos relacional principal. Cada microservicio tiene su propio schema o base de datos independiente (aislamiento de datos entre servicios). PostGIS se usa en `incident-service` y `analytics-service` para almacenar y consultar coordenadas geográficas.

### pgvector
Extensión de PostgreSQL usada por `ai-classifier-service` para almacenar embeddings de la base de conocimiento (incidencias de referencia, normativa municipal) y ejecutar búsquedas por similitud semántica.

### Redis
Usado por `notification-service` para garantizar idempotencia en el envío de notificaciones (evitar duplicados ante reintentos de Kafka). También puede usarse como caché de perfiles de usuario en `notification-service`.

---

## Estructura de repositorio GitHub

```
citypulse/
├── incident-service/
├── user-service/
├── ai-classifier-service/
├── routing-service/
├── department-service/
├── notification-service/
├── analytics-service/
├── infrastructure/
│   ├── kafka/               # docker-compose local
│   ├── postgres/            # scripts de inicialización
│   └── redis/
├── k8s/
│   ├── base/                # manifests Kubernetes por servicio
│   └── overlays/            # kustomize: local, staging, prod
├── .github/
│   └── workflows/           # GitHub Actions CI/CD
└── docker-compose.yml       # entorno local completo
```

### Estructura interna de cada microservicio (arquitectura hexagonal)

```
{service}/
├── src/main/java/com/citypulse/{service}/
│   ├── domain/
│   │   ├── model/           # entidades y value objects (Java puro)
│   │   ├── port/
│   │   │   ├── in/          # casos de uso (interfaces)
│   │   │   └── out/         # puertos de salida (interfaces)
│   │   └── service/         # lógica de negocio (implementa puertos in)
│   ├── application/
│   │   └── usecase/         # orquestación de casos de uso
│   └── infrastructure/
│       ├── web/             # controllers REST
│       ├── persistence/     # repositorios JPA / PostGIS
│       ├── messaging/       # Kafka producers y consumers
│       └── ai/              # adaptadores Spring AI (solo donde aplica)
├── src/main/resources/
│   └── application.yml
├── src/test/
│   ├── unit/                # tests de dominio (sin Spring)
│   └── integration/         # tests con Testcontainers
├── Dockerfile
└── pom.xml
```

---

## Stack tecnológico

| Capa | Tecnología |
|---|---|
| Framework base | Spring Boot 4.1 |
| IA / RAG / agentes | Spring AI |
| Mensajería | Apache Kafka |
| Persistencia relacional | PostgreSQL 18 + PostGIS |
| Búsqueda vectorial | pgvector |
| Caché / idempotencia | Redis |
| Contenedores | Docker |
| Orquestación | Kubernetes (k3s local, AKS/GKE en cloud) |
| CI/CD | GitHub Actions |
| Tests de integración | Testcontainers |
| Documentación API | SpringDoc OpenAPI (Swagger UI) |
| Email | SMTP / SendGrid |
| SMS | Twilio / AWS SNS |

---

## Plan de iteraciones

### Iteración 1 — MVP sincrónico
`incident-service` + `user-service` + `notification-service` comunicándose de forma síncrona (HTTP). Sin Kafka, sin IA. El ciudadano reporta una incidencia y recibe un email de confirmación. Desplegable con Docker Compose.

### Iteración 2 — Eventos asíncronos
Introducir Kafka. `incident-service` publica `incident.reported`. `notification-service` lo consume. Añadir `routing-service` y `department-service` con los tres departamentos. Flujo completo desde reporte hasta resolución.

### Iteración 3 — Inteligencia artificial
Añadir `ai-classifier-service` con RAG y pgvector. El clasificador enriquece la incidencia antes de que el `routing-service` la enrute. Ajustar el `routing-service` para usar la clasificación IA.

### Iteración 4 — Analytics y reporting
Añadir `analytics-service` con PostGIS y el agente redactor de Spring AI. Generación de informes ejecutivos en PDF bajo demanda.

### Iteración 5 — Kubernetes y CI/CD
Manifests Kubernetes para todos los servicios. Kustomize para gestionar entornos. Pipeline GitHub Actions: build → test → build image → push registry → deploy.

---

## Consideraciones de diseño

**Aislamiento de datos:** cada microservicio es dueño exclusivo de su base de datos. Ningún servicio accede directamente a la base de datos de otro; la comunicación de datos es siempre a través de la API o de eventos Kafka.

**Idempotencia:** los consumidores Kafka deben ser idempotentes. El `notification-service` usa Redis para registrar qué notificaciones ya han sido enviadas y evitar duplicados en caso de reintento.

**Trazabilidad:** todos los eventos incluyen un identificador de correlación (`correlationId`) que permite trazar el ciclo de vida completo de una incidencia a través de los logs de todos los servicios.

**Desacoplamiento del canal de notificación:** el `notification-service` decide en tiempo de ejecución el canal de envío consultando las preferencias del ciudadano. La lógica de negocio de notificación no conoce si el ciudadano tiene email o teléfono; esa decisión la toma el adaptador correspondiente.

**Seguridad:** la API pública (ciudadano) está protegida con JWT gestionado por `user-service`. La API interna (departamentos, analytics) usa autenticación por API key o mTLS en Kubernetes.

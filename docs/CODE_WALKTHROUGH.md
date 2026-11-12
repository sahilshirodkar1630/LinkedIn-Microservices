# Code Walkthrough — LinkedIn Microservices

This document explains the code of every service so that someone opening the repository for the first time can
understand **what each file does, why it exists and how the services cooperate**. It assumes basic Java; Spring and
microservice ideas are explained as they appear.

Read the [README](../README.md) first for the big picture.

---

## Contents

1. [Microservices in five minutes](#1-microservices-in-five-minutes)
2. [Common layout of every service](#2-common-layout-of-every-service)
3. [Discovery Server (Eureka)](#3-discovery-server-eureka)
4. [API Gateway](#4-api-gateway)
5. [User Service](#5-user-service)
6. [Identity propagation: X-User-Id, ThreadLocal and Feign](#6-identity-propagation-x-user-id-threadlocal-and-feign)
7. [Connections Service (Neo4j)](#7-connections-service-neo4j)
8. [Posts Service](#8-posts-service)
9. [Notification Service](#9-notification-service)
10. [Kafka in this project](#10-kafka-in-this-project)
11. [Error handling](#11-error-handling)
12. [Configuration reference](#12-configuration-reference)
13. [End-to-end traces](#13-end-to-end-traces)
14. [Glossary](#14-glossary)

---

## 1. Microservices in five minutes

A **monolith** is one application with one database. A **microservice** system splits the application into small
services, each with its own codebase, process and database, talking over the network.

What that requires — and where each answer lives in this repo:

| Problem | Solution here |
|---|---|
| How does a client reach many services through one address? | **API Gateway** (Spring Cloud Gateway) |
| How do services find each other's host/port, which can change? | **Service discovery** (Netflix Eureka) |
| How do we authenticate without every service checking passwords? | Gateway validates a **JWT** once, forwards `X-User-Id` |
| How does one service ask another for data *now*? | **OpenFeign** HTTP client (synchronous) |
| How do services react to things that happened elsewhere? | **Kafka** events (asynchronous) |
| How does each service store data in the best form? | **Database per service** — PostgreSQL ×3, Neo4j ×1 |

---

## 2. Common layout of every service

Every business service is a normal Spring Boot app with the same packages:

```
com.sahil.linkedInProject.<service>
├── <Service>Application.java   main class (@SpringBootApplication, sometimes @EnableFeignClients)
├── controller/                 REST endpoints (@RestController)
├── service/                    business logic (@Service)
├── repository/                 Spring Data interfaces (JPA or Neo4j)
├── entity/                     tables (@Entity) or graph nodes (@Node)
├── dto/                        request/response shapes
├── event/                      Kafka message classes
├── config/                     beans: ModelMapper, Kafka topics
├── auth/                       X-User-Id handling (Posts & Connections)
├── client/                     Feign clients (Posts)
├── consumer/                   @KafkaListener classes (Connections & Notification)
└── exception/                  ApiError, custom exceptions, @RestControllerAdvice
```

Quick Spring reminders:

- `@RestController` + `@RequestMapping("/core")` → class handles URLs under `/core`.
- `@Service`, `@Component`, `@Configuration` → Spring creates and injects these objects (beans).
- `@RequiredArgsConstructor` (Lombok) → constructor for `final` fields, which Spring uses for dependency injection.
- `server.servlet.context-path=/posts` → *every* URL of that service starts with `/posts`.

---

## 3. Discovery Server (Eureka)

**Files:** `DiscoverServer/src/main/java/.../DiscoverServerApplication.java`, `application.properties`

```java
@EnableEurekaServer
@SpringBootApplication
public class DiscoverServerApplication { ... }
```

```properties
spring.application.name=discover-server
server.port=8761
eureka.client.register-with-eureka=false   # the registry does not register itself
eureka.client.fetch-registry=false         # nor does it need to read the registry
```

How it works:

1. Each service has `spring-cloud-starter-netflix-eureka-client` on its classpath and a
   `spring.application.name` (e.g. `posts-service`).
2. On start-up it **registers** with Eureka at `http://localhost:8761/eureka`: "posts-service is at
   192.168.1.5:9010". `eureka.instance.prefer-ip-address=true` makes it register its IP instead of its hostname.
3. It then sends a **heartbeat** every 30 s. If heartbeats stop, Eureka removes the instance.
4. Clients (gateway, Feign) ask Eureka for the instances of a name and pick one — **client-side load balancing**.

The dashboard at <http://localhost:8761> lists every registered instance.

---

## 4. API Gateway

**Files:** `APIGateway/.../ApiGatewayApplication.java`, `JwtService.java`, `filter/AuthenticationFilter.java`,
`application.yml`

Spring Cloud Gateway is built on **Spring WebFlux** (reactive, non-blocking) — which is why the filter code uses
`exchange` and `Mono` rather than `HttpServletRequest`.

### Routes (`application.yml`)

```yaml
spring:
  cloud:
    gateway:
      routes:
        - id: user-service
          uri: lb://USER-SERVICE            # "lb://" = resolve through Eureka + load-balance
          predicates:
            - Path=/api/v1/users/**         # which requests this route matches
          filters:
            - StripPrefix=2                 # drop "/api/v1" before forwarding
        - id: posts-service
          uri: lb://POSTS-SERVICE
          predicates:
            - Path=/api/v1/posts/**
          filters:
            - StripPrefix=2
            - name: AuthenticationFilter    # our custom JWT filter
        - id: connections-service
          uri: lb://CONNECTIONS-SERVICE
          predicates:
            - Path=/api/v1/connections/**
          filters:
            - StripPrefix=2
            - name: AuthenticationFilter
```

**Path translation example:** `GET /api/v1/posts/core/7` → `StripPrefix=2` removes `api` and `v1` → `/posts/core/7`
→ the posts service has context path `/posts`, so its controller sees `/core/7`.

The user-service route has **no** `AuthenticationFilter`, because sign-up and login must work without a token.

### `AuthenticationFilter`

```java
public class AuthenticationFilter extends AbstractGatewayFilterFactory<AuthenticationFilter.Config> {
    public GatewayFilter apply(Config config) {
        return (exchange, chain) -> {
            String tokenHeader = exchange.getRequest().getHeaders().getFirst("Authorization");
            if (tokenHeader == null || !tokenHeader.startsWith("Bearer")) {
                exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
                return exchange.getResponse().setComplete();            // stop here, 401
            }
            String token = tokenHeader.split("Bearer ")[1];
            try {
                String userId = jwtService.getUserIdFromToken(token);   // verifies signature + expiry
                ServerWebExchange mutated = exchange.mutate()
                        .request(r -> r.header("X-User-Id", userId))   // add identity header
                        .build();
                return chain.filter(mutated);                           // continue to the service
            } catch (JwtException e) {
                exchange.getResponse().setStatusCode(HttpStatus.UNAUTHORIZED);
                return exchange.getResponse().setComplete();
            }
        };
    }
    static class Config { }   // no options; required by AbstractGatewayFilterFactory
}
```

- Extending `AbstractGatewayFilterFactory` is what lets the YAML refer to it as `name: AuthenticationFilter`.
- `exchange` holds the request and response; `chain.filter(...)` passes the request on.
- `exchange.mutate()` creates a modified copy of the request (requests are immutable in WebFlux).

### `JwtService` (gateway)

Only *reads* tokens:

```java
Claims claims = Jwts.parser().verifyWith(getSecretKey()).build()
                    .parseSignedClaims(token).getPayload();
return claims.getSubject();      // the userId the user service put in "sub"
```

`getSecretKey()` builds an HMAC key from `jwt.secretKey`. It must be the **same secret** the user service signs
with, otherwise verification fails.

---

## 5. User Service

**Port 9020, context path `/users`, database `userDB`.**

### `entity/User.java`

```java
@Entity @Table(name = "users")
public class User {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY) private Long id;
    @Column(nullable = false)               private String name;
    @Column(nullable = false, unique = true) private String email;
    @Column(nullable = false)               private String password;   // BCrypt hash
}
```

### `repository/UserRepository.java`

```java
boolean existsByEmail(String email);
Optional<User> findByEmail(String email);
```

Spring Data generates the SQL from the method names (`SELECT count(*) > 0 … WHERE email = ?`, etc.).

### `controller/UserController.java` — `@RequestMapping("/auth")`

| Endpoint | Calls | Returns |
|---|---|---|
| `POST /auth/signup` | `authService.signUp(dto)` | `201` + `UserDto {id, name, email}` (no password) |
| `POST /auth/login` | `authService.login(dto)` | `200` + JWT string |

### `service/AuthService.java`

**Sign-up**

```java
if (userRepository.existsByEmail(dto.getEmail())) throw new BadRequestException("User already exists");
User user = modelMapper.map(dto, User.class);
user.setPassword(BCrypt.hash(dto.getPassword()));
user = userRepository.save(user);

UserCreatedEvent event = UserCreatedEvent.builder().userId(user.getId()).name(user.getName()).build();
userCreatedKafkaTemplate.send("user_created_topic", event);   // tell the rest of the system
return modelMapper.map(user, UserDto.class);
```

The user service does not know or care who listens to `user_created_topic` — today it is the connections service;
tomorrow a "welcome email" service could subscribe without any change here.

**Login**

```java
User user = userRepository.findByEmail(dto.getEmail())
        .orElseThrow(() -> new BadRequestException("Incorrect email or password"));
if (!BCrypt.match(dto.getPassword(), user.getPassword()))
        throw new BadRequestException("Incorrect email or password");
return jwtService.generateAccessToken(user);
```

The same message is used for "no such email" and "wrong password" so an attacker cannot discover which emails are
registered.

### `utils/BCrypt.java`

A thin wrapper over the jBCrypt library: `hash(s)` = `hashpw(s, gensalt())`, `match(plain, hash)` = `checkpw`.
BCrypt adds a random salt to every hash and is deliberately slow, which makes brute-forcing stolen hashes expensive.

### `service/JwtService.java` (user service)

```java
Jwts.builder()
    .subject(user.getId().toString())
    .claim("email", user.getEmail())
    .issuedAt(new Date())
    .expiration(new Date(System.currentTimeMillis() + 1000*60*100))   // 100 minutes
    .signWith(getSecretKey())
    .compact();
```

### `config/KafkaConfig.java`

Declares `NewTopic` beans; Spring Kafka's admin client creates them on start-up if missing:

```java
new NewTopic("user_created_topic", 3, (short) 1);   // 3 partitions, replication factor 1
```

### `application.yml` — producer settings

```yaml
spring.kafka:
  bootstrap-servers: localhost:29092
  producer:
    key-serializer: org.apache.kafka.common.serialization.LongSerializer
    value-serializer: org.springframework.kafka.support.serializer.JsonSerializer
```

`JsonSerializer` turns the event object into JSON and adds a **type header** naming the Java class
(`com.sahil.linkedInProject.userService.event.UserCreatedEvent`) — important for consumers (section 10).

---

## 6. Identity propagation: X-User-Id, ThreadLocal and Feign

Posts and Connections both contain the same four classes in `auth/`. They answer: *"inside a service method, how do
I know who is calling?"*

### `RequestInterceptor` — read the header on the way in

```java
public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
    String userId = request.getHeader("X-User-Id");
    AuthContextHolder.setCurrentUserId(Long.valueOf(userId));
    return true;                      // continue to the controller
}
public void afterCompletion(...) {
    AuthContextHolder.clear();        // always clean up
}
```

A Spring MVC `HandlerInterceptor` runs before (`preHandle`) and after (`afterCompletion`) every controller call.

### `AuthContextHolder` — remember it for this request

```java
public class AuthContextHolder {
    private static final ThreadLocal<Long> currentUserId = new ThreadLocal<>();
    public static Long getCurrentUserId() { return currentUserId.get(); }
    static void setCurrentUserId(Long id) { currentUserId.set(id); }
    static void clear() { currentUserId.remove(); }
}
```

A **`ThreadLocal`** is a variable where each thread sees its own value. A servlet server handles each request on
one thread, so the user id set in `preHandle` is visible anywhere in that request (controller → service → Feign
call) without passing it as a parameter. `clear()` matters because threads are **reused** from a pool — without it
the next request on that thread could see the previous user's id.

### `WebMvcConfig` — register the interceptor

```java
public void addInterceptors(InterceptorRegistry registry) {
    registry.addInterceptor(requestInterceptor);
}
```

### `FeignClientInterceptor` — pass it on to the next service

```java
public void apply(RequestTemplate requestTemplate) {
    Long userId = AuthContextHolder.getCurrentUserId();
    if (userId != null) requestTemplate.header("X-User-Id", userId.toString());
}
```

Feign calls this before every outgoing request, so when Posts calls Connections, Connections still knows who the
original caller is.

---

## 7. Connections Service (Neo4j)

**Port 9030, context path `/connections`, Neo4j at `neo4j://localhost:7687`.** `@EnableFeignClients` is present on
the main class.

### Why a graph database?

In a relational database, "friends of friends" is a self-join on a `connections` table, and each extra degree adds
another join. In a graph database, people are **nodes** and connections are **relationships** stored as direct
pointers, so following them is cheap and queries read like the question:

```
(Asha)-[:CONNECTED_TO]-(Ravi)-[:CONNECTED_TO]-(Meera)
```

### `entity/Person.java`

```java
@Node                      // a Neo4j node with label "Person"
public class Person {
    @Id @GeneratedValue private Long id;   // Neo4j's internal id
    private Long userId;                   // the id from the user service
    private String name;
}
```

### Relationship types used

| Relationship | Direction | Meaning |
|---|---|---|
| `REQUESTED_TO` | sender → receiver | A pending connection request |
| `CONNECTED_TO` | created sender → receiver, **queried in both directions** | An accepted connection |

### `repository/PersonRepository.java` — Cypher queries

`Neo4jRepository<Person, Long>` gives CRUD; `findByUserId` is derived from the name. The rest use **Cypher**, Neo4j's
query language; `$userId` binds a method parameter.

```cypher
-- getFirstDegreeConnections(userId): direct connections, either direction (note "-" not "->")
MATCH (personA:Person)-[:CONNECTED_TO]-(personB:Person)
WHERE personA.userId = $userId
RETURN personB

-- connectionRequestExists(senderId, receiverId)
MATCH (p1:Person)-[r:REQUESTED_TO]->(p2:Person)
WHERE p1.userId = $senderId AND p2.userId = $receiverId
RETURN count(r) > 0

-- alreadyConnected(senderId, receiverId)  (undirected)
MATCH (p1:Person)-[r:CONNECTED_TO]-(p2:Person)
WHERE p1.userId = $senderId AND p2.userId = $receiverId
RETURN count(r) > 0

-- addConnectionRequest
MATCH (p1:Person), (p2:Person)
WHERE p1.userId = $senderId AND p2.userId = $receiverId
CREATE (p1)-[:REQUESTED_TO]->(p2)

-- acceptConnectionRequest: replace the request with a connection
MATCH (p1:Person)-[r:REQUESTED_TO]->(p2:Person)
WHERE p1.userId = $senderId AND p2.userId = $receiverId
DELETE r
CREATE (p1)-[:CONNECTED_TO]->(p2)

-- rejectConnectionRequest: just delete the request
MATCH (p1:Person)-[r:REQUESTED_TO]->(p2:Person)
WHERE p1.userId = $senderId AND p2.userId = $receiverId
DELETE r
```

### `service/ConnectionsService.java` — the request workflow

| Method | Caller is | Checks (each failure throws) | Action |
|---|---|---|---|
| `sendConnectionRequest(receiverId)` | the **sender** | not yourself; no pending request; not already connected | `addConnectionRequest` |
| `acceptConnectionRequest(senderId)` | the **receiver** | not yourself; not already connected; a request from sender exists | `acceptConnectionRequest` |
| `rejectConnectionRequest(senderId)` | the **receiver** | not yourself; a request exists | `rejectConnectionRequest` |
| `getFirstDegreeConnectionsOfUser(userId)` | anyone | – | `getFirstDegreeConnections` |

The caller's id always comes from `AuthContextHolder`, i.e. from the verified token — never from the request body.

### `service/PersonService.java`

`createPerson(userId, name)` builds and saves a `Person` node.

### `consumer/UserServiceConsumer.java`

```java
@KafkaListener(topics = "user_created_topic")
public void handlePersonCreated(UserCreatedEvent event) {
    personService.createPerson(event.getUserId(), event.getName());
}
```

Spring Kafka polls the topic and calls this method for every message, already deserialized from JSON.

### `controller/ConnectionsController.java` — `@RequestMapping("/core")`

`GET /{userId}/first-degree` (declares `@RequestHeader("X-User-Id")`, only logged), `POST /request/{userId}`,
`POST /accept/{userId}`, `POST /reject/{userId}` — the POSTs return `204 No Content`.

### The `userService.event.UserCreatedEvent` copy

This class lives in package `com.sahil.linkedInProject.userService.event` **inside the connections service**. That
matches the type header written by the producer, so the JSON deserializer finds a class with exactly that name
locally. See section 10.

---

## 8. Posts Service

**Port 9010, context path `/posts`, database `postDB`.** Main class has `@EnableFeignClients`.

### Entities

```java
@Entity public class Post {
    @Id @GeneratedValue(strategy = IDENTITY) Long id;
    @Column(nullable = false) String content;
    @Column(nullable = false) Long userId;      // author — just an id, no foreign key to another DB
    @CreationTimestamp LocalDateTime createdAt;
}

@Entity @Table(name = "post_likes") public class PostLike {
    @Id @GeneratedValue(strategy = IDENTITY) Long id;
    @Column(nullable = false) Long userId;      // who liked
    @Column(nullable = false) Long postId;      // which post
    @CreationTimestamp LocalDateTime createdAt;
}
```

Note `userId` is a plain number: users live in a different database, so there can be no SQL foreign key across
services.

### Repositories

```java
List<Post> findByUserId(Long userId);                         // PostRepository
boolean existsByUserIdAndPostId(Long userId, Long postId);    // PostLikeRepository
void deleteByUserIdAndPostId(Long userId, Long postId);
```

### `client/ConnectionsServiceClient.java` — OpenFeign

```java
@FeignClient(name = "connections-service", path = "/connections")
public interface ConnectionsServiceClient {
    @GetMapping("/core/{userId}/first-degree")
    List<PersonDto> getFirstDegreeConnections(@PathVariable Long userId);
}
```

You write only an interface; Feign generates the HTTP client. `name = "connections-service"` is looked up in
Eureka, `path` is the context path, and the JSON array is mapped to `List<PersonDto>`. The
`FeignClientInterceptor` adds `X-User-Id`, which the connections service needs.

### `service/PostService.java`

**`createPost(dto, userId)`**

```java
Post post = modelMapper.map(dto, Post.class);
post.setUserId(userId);
post = postRepository.save(post);

List<PersonDto> connections = connectionsServiceClient.getFirstDegreeConnections(userId);  // sync call
for (PersonDto person : connections) {
    PostCreated event = PostCreated.builder()
            .postId(post.getId())
            .content(post.getContent())
            .ownerUserId(person.getUserId())
            .userId(post.getUserId())
            .build();
    postCreatedKafkaTemplate.send("post_created_topic", event);                         // async fan-out
}
return modelMapper.map(post, PostDto.class);
```

This is the **fan-out-on-write** idea: when you post, one event per connection is produced so each can be notified.

`getPostById(id)` (404 if missing) and `getAllPostsOfUser(userId)` are simple reads mapped to `PostDto`.

### `service/PostLikeService.java` — both methods `@Transactional`

**`likePost(postId)`**: current user from `AuthContextHolder` → post must exist (404) → must not be liked already
(400) → save `PostLike` → publish `PostLiked { likedByUserId, ownerUserId = post author, postId }`.

**`unlikePost(postId)`**: post must exist → must be liked (400 otherwise) → `deleteByUserIdAndPostId`. Derived
delete queries must run inside a transaction, which is why `@Transactional` is needed here.

### Controllers

- `PostController` (`/core`): `POST /` create, `GET /{postId}`, `GET /users/{userId}/allPosts`.
- `PostLikesController` (`/likes`): `POST /{postId}` like, `DELETE /{postId}` unlike — both `204`.

### `config/KafkaConfig.java`

Creates `post_created_topic` and `post_liked_topic` (3 partitions each).

---

## 9. Notification Service

**Port 9040, context path `/notifications`, database `notificationDB`.** It has no controller: it is a pure
**event consumer**.

### `entity/Notification.java`

```java
@Entity public class Notification {
    @Id @GeneratedValue(strategy = IDENTITY) Long id;
    Long userId;                     // who should see it
    String message;
    @CreationTimestamp LocalDateTime createdAt;
}
```

### `consumer/PostsConsumer.java`

```java
@KafkaListener(topics = "post_created_topic")
public void handlePostCreated(PostCreated e) {
    String message = String.format("your connection with id: %d has created this post: %s",
                                   e.getOwnerUserId(), e.getContent());
    notificationService.addNotification(
        Notification.builder().message(message).userId(e.getUserId()).build());
}

@KafkaListener(topics = "post_liked_topic")
public void handlePostLiked(PostLiked e) {
    String message = String.format("User with id: %d has liked this post with id : %d",
                                   e.getLikedByUserId(), e.getPostId());
    notificationService.addNotification(
        Notification.builder().message(message).userId(e.getOwnerUserId()).build());
}
```

### `service/NotificationService.java`

`addNotification` saves the notification (a comment marks where an email sender would go).

### Event classes

`com.sahil.linkedInProject.postsService.event.PostCreated` and `.PostLiked` — local copies under the producer's
package name, for the same reason as in section 7.

---

## 10. Kafka in this project

### Vocabulary

| Term | Meaning |
|---|---|
| **Broker** | The Kafka server (here one, at `localhost:29092`) |
| **Topic** | A named stream of messages, e.g. `post_liked_topic` |
| **Partition** | A topic is split into partitions (3 here) so it can be consumed in parallel; order is kept *within* a partition |
| **Producer** | Writes messages — `KafkaTemplate.send(topic, value)` |
| **Consumer group** | Consumers sharing a `group-id` split the partitions between them; each message goes to one member of each group |
| **Offset** | A consumer's position in a partition; Kafka remembers it, so a restarted service continues where it stopped |

Each consumer uses `group-id: ${spring.application.name}`, so if you start two instances of the notification
service they share the work instead of both saving every notification.

### Serialization and the "same package" trick

Producer config: `LongSerializer` keys + `JsonSerializer` values. The JSON serializer adds a header
`__TypeId__ = com.sahil.linkedInProject.postsService.event.PostLiked`.

Consumer config:

```yaml
spring.kafka.consumer:
  key-deserializer: org.apache.kafka.common.serialization.LongDeserializer
  value-deserializer: org.springframework.kafka.support.serializer.JsonDeserializer
  properties:
    spring.json:
      trusted.packages: com.sahil.linkedInProject.*
```

The `JsonDeserializer` reads the type header and instantiates that class — but only if its package is **trusted**
(protection against deserializing arbitrary classes). Because the consumer has a class with the identical
fully-qualified name, deserialization just works. Alternatives are a shared "events" library module, or type
mappings (`spring.json.type.mapping`).

### Why Kafka instead of direct calls?

- **Decoupling** — the producer does not know the consumers.
- **Resilience** — if a consumer is down, messages wait in the topic.
- **Scalability** — add consumer instances to process partitions in parallel.
- **Extensibility** — new features subscribe to existing topics without touching producers.

---

## 11. Error handling

Each service has `exception/` with:

- `ApiError { timeStamp, error, statusCode }` — the JSON body for errors.
- `ResourceNotFoundException`, `BadRequestException` — extend `RuntimeException`.
- `GlobalExceptionHandler` (`@RestControllerAdvice`):

| Exception | Status |
|---|---|
| `ResourceNotFoundException` | 404 |
| `BadRequestException` | 400 |
| `RuntimeException` (User, Connections, Notification services) | 500 |

In the Posts service the `RuntimeException` handler's annotation is commented out, so other exceptions fall back to
Spring's default error response.

---

## 12. Configuration reference

| Service | `spring.application.name` | Port | Context path | Data store | Kafka role |
|---|---|---|---|---|---|
| DiscoverServer | `discover-server` | 8761 | – | – | – |
| APIGateway | `api-gateway` | 8080 | – | – | – |
| userService | `user-service` | 9020 | `/users` | `jdbc:postgresql://localhost:5432/userDB` | producer |
| postsService | `posts-service` | 9010 | `/posts` | `jdbc:postgresql://localhost:5432/postDB` | producer |
| ConnectionsService | `connections-service` | 9030 | `/connections` | `neo4j://localhost:7687` | consumer |
| NotificationService | `notification-service` | 9040 | `/notifications` | `jdbc:postgresql://localhost:5432/notificationDB` | consumer |

Other settings used across services:

- `spring.jpa.hibernate.ddl-auto=create` — tables are recreated from the entities at every start (development
  setting; use migrations in production).
- `eureka.client.service-url.defaultZone=http://localhost:8761/eureka` — where to register (it is also the
  default).
- `jwt.secretKey` — in the gateway and user service; the two values must be equal.

Versions: Java 21; Spring Boot 3.3.x with Spring Cloud 2023.0.5 for most services; the Notification Service uses
Spring Boot 4.1 with Spring Cloud 2025.1.

---

## 13. End-to-end traces

### A. Sign-up

1. `POST /api/v1/users/auth/signup` hits the gateway → route `user-service` (no auth filter) → `StripPrefix=2` →
   `lb://USER-SERVICE` resolved by Eureka → `/users/auth/signup`.
2. `UserController.signUp` → `AuthService.signUp`: email unique? → BCrypt hash → save in `userDB`.
3. `KafkaTemplate.send("user_created_topic", {userId, name})`.
4. Connections Service `UserServiceConsumer` receives it → `PersonService.createPerson` → `(:Person)` node in Neo4j.

### B. Connecting

1. Asha calls `POST /api/v1/connections/core/request/2` with her token.
2. Gateway `AuthenticationFilter` validates the token, adds `X-User-Id: 1`.
3. Connections `RequestInterceptor` stores `1` in `AuthContextHolder`.
4. `ConnectionsService.sendConnectionRequest(2)` runs its checks → Cypher `CREATE (p1)-[:REQUESTED_TO]->(p2)`.
5. Ravi calls `POST .../accept/1` → the `REQUESTED_TO` edge is replaced by `CONNECTED_TO`.

### C. Posting

1. `POST /api/v1/posts/core {content}` → gateway authenticates → Posts `PostController.createPost`.
2. `PostService.createPost` saves the post in `postDB`.
3. Feign `ConnectionsServiceClient.getFirstDegreeConnections` → Eureka lookup → HTTP GET to Connections with
   `X-User-Id` forwarded → Cypher returns the connected `Person`s.
4. One `post_created_topic` event per connection.
5. Notification Service `PostsConsumer.handlePostCreated` → `Notification` saved in `notificationDB`.

### D. Liking

1. `POST /api/v1/posts/likes/1` → `PostLikeService.likePost` → duplicate check → `PostLike` saved.
2. `post_liked_topic {postId, ownerUserId, likedByUserId}` → Notification Service stores a notification for the
   post's owner.

---

## 14. Glossary

| Term | Meaning |
|---|---|
| **API Gateway** | The single front door that routes requests to services and applies cross-cutting rules (auth) |
| **Eureka** | Netflix's service registry: services register, clients look them up by name |
| **`lb://`** | Gateway URI scheme meaning "load-balanced lookup through the registry" |
| **OpenFeign** | Declarative HTTP client: write an interface, get an implementation |
| **Kafka** | Distributed, durable log of messages (events) organised in topics |
| **Event-driven** | Services publish facts ("user created") and others react to them |
| **Polyglot persistence** | Using different database types for different data |
| **Neo4j / Cypher** | Graph database / its query language |
| **JWT** | Signed token carrying claims such as the user id (`sub`) |
| **BCrypt** | Salted, slow password hashing |
| **ThreadLocal** | A variable with a separate value per thread — here, per request |
| **HandlerInterceptor** | Spring MVC hook that runs before/after controller methods |
| **StripPrefix** | Gateway filter removing leading path segments before forwarding |
| **Fan-out on write** | Producing one message per recipient at the moment something is created |
| **Transactional outbox** | Pattern that writes events to the DB in the same transaction and publishes them afterwards, guaranteeing consistency |

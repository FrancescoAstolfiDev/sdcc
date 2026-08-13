# Spiegazione dettagliata — Week 1: Skeleton & RPC Contracts

Questo documento spiega, file per file, cosa fa ogni parte del codice generato per la Settimana 1 del progetto B5, e perché è stata scritta in quel modo, con riferimento alle sezioni della specifica tecnica (`Progetto_B5_Full_Technical_Spec_EN.pdf`).

Obiettivo della Settimana 1 (da `md-week1.md`): struttura del modulo Go, contratti Protocol Buffer, stub eseguibili per i 4 microservizi, librerie core integrate, `docker-compose.yml` valido per verificare la connettività di rete Docker. **Non** è previsto in questa fase implementare la logica reale di Raft, routing, snapshot, ecc. — quella arriva nelle settimane successive (§10.7 della spec: prima strutture dati/log in isolamento, poi elezioni/replica su trasporto finto, poi gRPC reale, poi persistenza, poi snapshot).

---

## 1. `go.mod`

```go
module b5-kvstore

go 1.22

require (
	github.com/sony/gobreaker v0.5.0
	google.golang.org/grpc v1.64.0
	google.golang.org/protobuf v1.34.1
)
```

- `module b5-kvstore`: nome del modulo Go, come richiesto esplicitamente da `md-week1.md`. Tutti gli import interni del progetto partiranno da questo prefisso (es. `b5-kvstore/pkg/pb`).
- `go 1.22`: versione minima del toolchain Go richiesta.
- `require`: le tre librerie "core" citate nella spec:
  - `google.golang.org/grpc` — comunicazione interna gRPC obbligatoria tra tutti i servizi tranne Client↔Proxy (§2, MUST).
  - `google.golang.org/protobuf` — runtime dei messaggi generati da `.proto`.
  - `github.com/sony/gobreaker` — libreria consigliata esplicitamente dalla spec per il pattern Circuit Breaker (§8), invece di implementarlo a mano.
- **Nota importante**: questo file è stato scritto a mano perché in questo ambiente non è disponibile il binario `go`. Non esiste ancora un `go.sum`. Al primo `make build` (o `go mod tidy`) sulla tua macchina/EC2, Go scaricherà le versioni esatte e genererà `go.sum` — le versioni indicate sono un punto di partenza plausibile, non sono state verificate da un `go mod tidy` reale.

---

## 2. I contratti Protocol Buffer (`api/proto/*.proto`)

Sono la parte più importante della Settimana 1: definiscono la "superficie" RPC autoritativa descritta nella §9 della spec. Sono stati trascritti **esattamente** dalla spec, con due adattamenti sistematici:
- i nomi dei campi sono stati convertiti da `camelCase` (come scritto nella spec) a `snake_case` (convenzione standard di Protobuf/Go, es. `candidateId` → `candidate_id`) — la spec stessa lo permette ("field names may be adapted to Go naming conventions", §9.5);
- `option go_package = "b5-kvstore/pkg/pb";` in ogni file: dice a `protoc-gen-go` di generare tutto il codice Go nella cartella `pkg/pb`, così i quattro `.proto` finiscono nello stesso package Go `pb` (i nomi dei messaggi non collidono tra loro, quindi è sicuro).

### 2.1 `consensus.proto`

Definisce il servizio **`Consensus`**, cioè le RPC scambiate *solo* tra nodi di consenso (leader ↔ follower/candidate), mai viste dal client esterno né dal proxy per le operazioni KV:

- `RequestVote(RequestVoteRequest) → RequestVoteReply`: usata da un nodo in stato Candidate per chiedere il voto agli altri nodi durante un'elezione (§3.4, §10.3).
- `AppendEntries(AppendEntriesRequest) → AppendEntriesReply`: usata dal Leader sia per replicare voci di log ai follower, sia come heartbeat quando `entries` è vuoto (§3.2, §3.3).
- `GetStatus(GetStatusRequest) → GetStatusReply`: endpoint leggero che ogni nodo espone per dire "chi sono, che ruolo ho, che termine ho" — usato dal Service Discovery per costruire la sua vista del cluster (§3.5, §7). Deve essere economico e non deve mai bloccarsi sull'attività di consenso.
- `InstallSnapshot(InstallSnapshotRequest) → InstallSnapshotReply`: **non è nella tabella §9.1**, ma è descritta a testo in §3.6 con la firma `(term, leaderId, lastIncludedIndex, lastIncludedTerm, data[]) → (term)`. È stata aggiunta al servizio `Consensus` perché è concettualmente una RPC tra nodi di consenso (il Leader la manda a un follower che ha bisogno di voci di log già compattate).

Messaggi rilevanti:
- `enum Role { FOLLOWER = 0; CANDIDATE = 1; LEADER = 2; }` — i tre stati possibili di un nodo (§3, tabella "Common node state"). È definito qui perché è condiviso anche da `discovery.proto` (un `NodeInfo` riporta il ruolo di un nodo).
- `LogEntry { term, index, command }` — la singola voce del log di Raft; `command` è `bytes` perché contiene un `KVCommand` serializzato (definito in `kv.proto`), non un tipo Protobuf diretto — questo disaccoppia il formato del log di Raft dal formato dei comandi applicativi.
- `AppendEntriesReply` include `conflict_index`/`conflict_term`, che la spec segnala come "optional but recommended" per il fast log back-off (§10.5) — sono stati inclusi da subito perché il contratto RPC deve già prevederli, anche se la logica che li popola arriverà dopo.

### 2.2 `kv.proto`

Definisce il servizio **`KVService`**, che è la RPC gRPC che il Client Proxy chiama internamente contro il Leader (mai esposta direttamente al client esterno, che parla solo REST/JSON — §2, §4.1):

- `Get`, `Put`, `Update`, `Delete`: notare che `Update` ha *la stessa forma* di `Put` (stesso messaggio `PutRequest`) ma semantica diversa (overwrite esplicito) — così come specificato testualmente in §9.2 ("same shape as Put, semantically an overwrite").
- `GetReply` ha un campo `redirect_leader`: se un nodo non-leader riceve per errore una richiesta di lettura, può rispondere indicando chi è il leader attuale invece di eseguire lui stesso l'operazione (coerente con la regola MUST NOT di §3.3: un follower non deve mai eseguire una scrittura destinata al leader).
- `WriteReply` ha `success`, `redirect_leader` e `commit_index` (quest'ultimo per testing/osservabilità, come indicato esplicitamente nella spec).
- `KVCommand` con `enum Op { PUT, DELETE, UPDATE }`: è il payload che viene serializzato dentro `LogEntry.command` in `consensus.proto`. È il "linguaggio" delle operazioni che finiscono nel log replicato.

### 2.3 `discovery.proto`

Definisce il servizio **`Discovery`**, interrogato dal Client Proxy e dal servizio Snapshot & Backup (mai dal client esterno, mai sulla data path — §7, MUST NOT):

- Un'unica RPC, `GetClusterView(Empty) → ClusterView`. Per il tipo `Empty` è stato importato `google/protobuf/empty.proto` (il tipo ben noto di Google) invece di definirne uno locale, per evitare duplicazione tra `discovery.proto` e `snapshot.proto` che ne hanno entrambi bisogno.
- `NodeInfo { node_id, address, role, term }` — usa `consensus.Role`, da cui l'`import "consensus.proto";` in testa al file: è un esempio concreto di come i quattro `.proto` non sono indipendenti ma si richiamano a vicenda dove la spec condivide dei tipi.
- `ClusterView { leader_address, followers[], all_nodes[] }` — esattamente come da §9.3: la vista che il Client Proxy usa per instradare le richieste e che il Circuit Breaker invalida quando serve (§8).

### 2.4 `snapshot.proto`

Definisce **due** servizi distinti, che nella spec vivono nella stessa sezione (§9.4/§9.5) ma hanno scopi e vincoli diversi:

- **`SnapshotTransfer`** (backup service ↔ leader o un follower): `GetLogStatus`, `StreamLogRange` (streaming server-side di `consensus.LogEntry` — da cui il secondo `import "consensus.proto";`), `ConfirmCompaction`. La spec sottolinea una distinzione importante, riportata come commento nel `.proto`: `StreamLogRange` può puntare a *qualsiasi* nodo (i dati sono già committati, quindi va bene la copia di un follower), mentre `GetLogStatus` e `ConfirmCompaction` devono restare vincolati al leader corrente, perché solo lui può decidere autoritativamente cosa è sicuro troncare adesso.
- **`SnapshotCatalog`** (interrogato da *qualunque* nodo di consenso, non solo dal leader): `GetLatestSnapshotInfo` e `FetchSnapshot` (streaming di `SnapshotChunk`). Questo è il meccanismo di "catch-up" periodico descritto in §5.3: ogni nodo, leader incluso, controlla periodicamente se esiste uno snapshot più recente della propria `lastApplied` e, se sì, lo scarica e ancora il proprio log a quel punto.

---

## 3. Gli stub eseguibili (`cmd/*/main.go`)

Sono quattro binari Go minimi, uno per ciascun servizio richiesto da §2 ("Exactly three distinct node types... plus" — in realtà quattro binari contando anche il discovery). **Nessuno di questi implementa ancora logica applicativa reale**: lo scopo dichiarato della Settimana 1 è solo verificare che i container possano avviarsi ed essere raggiungibili in rete Docker. Per questo ogni stub:

1. legge la propria configurazione da variabili d'ambiente (mai hard-coded — coerente con §10.6, che vieta di hard-codare gli indirizzi dei peer);
2. apre un listener di rete sulla propria porta;
3. registra un endpoint di **health check gRPC standard** (`grpc.health.v1`, libreria ufficiale `google.golang.org/grpc/health`) invece di implementare già i servizi `Consensus`/`KVService`/ecc., che richiedono il codice generato da `protoc` (non ancora eseguito in questo ambiente, vedi sotto);
4. si mette in ascolto e logga cosa sta facendo.

L'endpoint di health check non è un "trucco" fine a se stesso: è lo stesso meccanismo standard che userai più avanti per gli `healthcheck:` di Docker Compose e per verificare da fuori che un container sia vivo, quindi è codice che resta valido anche dopo l'implementazione reale.

### 3.1 `cmd/discovery/main.go`

```go
port := os.Getenv("DISCOVERY_PORT")
if port == "" { port = "8500" }

lis, err := net.Listen("tcp", ":"+port)
...
srv := grpc.NewServer()
healthSrv := health.NewServer()
healthpb.RegisterHealthServer(srv, healthSrv)
healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

log.Printf("discovery: listening on :%s (peers=%s)", port, os.Getenv("PEERS"))
srv.Serve(lis)
```

- `DISCOVERY_PORT` con default `8500` se non impostata (comodo per testare il binario fuori da Docker).
- `os.Getenv("PEERS")` viene solo **letto e loggato**, non ancora usato per interrogare i nodi (quello è lavoro di Week 2: polling periodico di `GetStatus()` su ogni peer, §7).
- `healthSrv.SetServingStatus("", ...SERVING)`: la stringa vuota `""` indica lo stato di servizio "generale" del processo (non di un servizio gRPC specifico) — è la convenzione usata dai probe di readiness/liveness gRPC standard.

### 3.2 `cmd/consensus-node/main.go`

Stessa struttura del discovery, con due differenze:

- legge anche `NODE_ID` (identificativo del nodo, che nella Week 2 diventerà il `candidateId`/`leaderId` usato nelle RPC `RequestVote`/`AppendEntries`) e usa la porta `CONSENSUS_PORT` (default `9000`);
- ha un commento `// TODO(week2): register the Consensus and KVService servers...` esplicito: qui è dove, dopo aver eseguito `make proto`, andrà registrato il vero server gRPC che implementa `pb.ConsensusServer` e `pb.KVServiceServer` (quest'ultimo solo se il nodo è leader).

Ogni container `consensus-node-N` nel `docker-compose.yml` passa un `NODE_ID` diverso (`node-1`, `node-2`, `node-3`) tramite variabili d'ambiente — lo stesso binario, comportamento diverso solo in base alla configurazione, esattamente come richiesto dalla spec ("All consensus nodes run identical code", §3).

### 3.3 `cmd/snapshot-backup/main.go`

Identico nella forma ai due precedenti (porta `SNAPSHOT_PORT`, default `8600`), logga anche `DISCOVERY_ADDR` perché in Week 2 questo servizio dovrà interrogare il Service Discovery per sapere chi è il leader attuale prima di ogni ciclo di compattazione (§5.1 — "never hard-code a leader address").

### 3.4 `cmd/client-proxy/main.go`

È il più diverso dagli altri tre, perché il Client Proxy è l'unico componente che parla **REST/JSON** verso l'esterno (§2, l'unica eccezione consentita al "tutto gRPC internamente"):

```go
func newBreakers(peers []string) map[string]*gobreaker.CircuitBreaker {
	breakers := make(map[string]*gobreaker.CircuitBreaker, len(peers))
	for _, peer := range peers {
		peer := peer
		breakers[peer] = gobreaker.NewCircuitBreaker(gobreaker.Settings{Name: peer})
	}
	return breakers
}
```

- Costruisce **una `gobreaker.CircuitBreaker` per ogni peer** nella lista `PEERS` — esattamente come richiesto in §8 ("one breaker instance per outbound connection to a known cluster node"). In questa fase i breaker vengono solo creati e contati/loggati, non ancora collegati a chiamate gRPC reali verso i nodi (quello arriva insieme al routing vero in Week 2, in `internal/proxy` + `internal/circuitbreaker`).
- `peer := peer` dentro il loop: è l'idioma classico Go per evitare il bug di cattura della variabile di loop per riferimento (rilevante soprattutto se in futuro le closure venissero eseguite in goroutine separate).
- Espone un server HTTP standard (`net/http`) invece di gRPC, con un solo endpoint per ora:

```go
mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"status": "ok", "peers": peers})
})
```

  Risponde con un JSON `{"status":"ok","peers":[...]}` — utile sia come probe di readiness sia come verifica manuale (`curl localhost:8080/healthz`) che il container è su, ha letto la sua configurazione ed è raggiungibile dall'host.
- Il commento `// TODO(week2): wire Get/Put/Update/Delete REST handlers...` segna dove arriveranno gli handler REST veri, che dovranno: decodificare il JSON del client, interrogare Service Discovery per il leader, aprire/riusare una connessione gRPC verso `KVService` passando per il breaker giusto, tradurre la risposta gRPC in JSON.

---

## 4. `pkg/pb/doc.go`

```go
// Package pb holds the Go code generated from api/proto/*.proto by
// `make proto` (protoc-gen-go + protoc-gen-go-grpc). It is intentionally
// empty until that command has been run — see the repository Makefile.
package pb
```

Serve solo a rendere `pkg/pb` un package Go valido (con un commento di pacchetto, come da convenzione Go) anche prima che `protoc` generi qualunque file `.pb.go` al suo interno. Va **sostituito/affiancato** dai file generati non appena esegui `make proto`: a quel punto in questa cartella compariranno file come `consensus.pb.go`, `consensus_grpc.pb.go`, `kv.pb.go`, ecc., con le struct dei messaggi e le interfacce client/server dei servizi.

---

## 5. `Makefile`

```makefile
PROTO_DIR := api/proto
PB_DIR := pkg/pb
COMPOSE := docker compose -f deployments/docker-compose.yml --env-file deployments/.env

proto:
	protoc \
		--go_out=$(PB_DIR) --go_opt=paths=source_relative \
		--go-grpc_out=$(PB_DIR) --go-grpc_opt=paths=source_relative \
		-I $(PROTO_DIR) \
		$(PROTO_DIR)/*.proto

tidy:
	go mod tidy

build: tidy
	go build ./...

up:
	$(COMPOSE) up --build

down:
	$(COMPOSE) down -v

logs:
	$(COMPOSE) logs -f

clean:
	rm -f $(PB_DIR)/*.pb.go
```

- `make proto`: invoca `protoc` con due plugin (`protoc-gen-go` per i messaggi, `protoc-gen-go-grpc` per client/server stub) puntando ai quattro file in `api/proto/`, con output diretto in `pkg/pb`. `paths=source_relative` mantiene la struttura dei nomi file coerente con i `.proto` sorgente. **Richiede** che `protoc`, `protoc-gen-go` e `protoc-gen-go-grpc` siano installati — non presenti in questo ambiente, per questo il target non è stato eseguito qui.
- `make tidy` / `make build`: normale ciclo Go (`go mod tidy` scarica le dipendenze reali e genera `go.sum`; `go build ./...` compila tutti i pacchetti, inclusi i quattro `cmd/*`).
- `make up` / `make down` / `make logs`: wrapper su `docker compose`, con `-f`/`--env-file` già puntati alle posizioni corrette dentro `deployments/`, così puoi lanciare `make up` dalla radice del repo senza doverti ricordare i percorsi.
- `make clean`: rimuove i `.pb.go` generati, utile se rigeneri i contratti da zero dopo aver modificato i `.proto`.

---

## 6. `deployments/.env`

```env
PEERS=consensus-node-1:9001,consensus-node-2:9002,consensus-node-3:9003
DISCOVERY_ADDR=discovery:8500
```

Corrisponde esattamente alla richiesta di §10.6/§11: **un'unica lista statica** di indirizzi dei peer, definita una sola volta e montata identicamente sia nei container dei nodi di consenso sia nel container di Service Discovery, per evitare "configuration drift" tra le due configurazioni. Gli hostname (`consensus-node-1`, ecc.) funzionano perché Docker Compose crea automaticamente una entry DNS per ogni servizio nella rete `b5-net` con lo stesso nome del servizio.

---

## 7. I quattro `Dockerfile.*`

Tutti e quattro seguono lo stesso pattern *multi-stage build*, cambia solo il binario finale compilato. Esempio (`Dockerfile.consensus`):

```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /src
COPY . .
RUN go mod tidy && CGO_ENABLED=0 go build -o /out/consensus-node ./cmd/consensus-node

FROM alpine:3.20
COPY --from=builder /out/consensus-node /usr/local/bin/consensus-node
ENTRYPOINT ["/usr/local/bin/consensus-node"]
```

- **Stage 1 (`builder`)**: parte da un'immagine con Go preinstallato, copia tutto il sorgente del repo, esegue `go mod tidy` (scarica le dipendenze e genera `go.sum` dentro l'immagine — scelta fatta apposta perché in questo repo non esiste ancora un `go.sum` verificato) e compila con `CGO_ENABLED=0` (binario statico, nessuna dipendenza da librerie C dinamiche — necessario per poterlo copiare in un'immagine Alpine minimale senza toolchain).
- **Stage 2 (finale)**: parte da un'immagine Alpine "pulita" (niente Go, niente sorgenti) e copia dentro **solo** il binario compilato. Questo mantiene l'immagine finale piccola e riduce la superficie d'attacco.
- `ENTRYPOINT` punta al binario giusto per ciascun servizio (`consensus-node`, `client-proxy`, `snapshot-backup`, `discovery`).

Gli altri tre file (`Dockerfile.proxy`, `Dockerfile.snapshot`, `Dockerfile.discovery`) sono identici a meno del nome del binario in `go build -o` e nell'`ENTRYPOINT`.

---

## 8. `deployments/docker-compose.yml`

Struttura, servizio per servizio:

- **`discovery`**: builda da `Dockerfile.discovery`, legge `deployments/.env`, imposta `DISCOVERY_PORT=8500`, nessuna porta pubblicata verso l'host (coerente con §11.2: "no consensus node, and no Service Discovery instance, is directly reachable from the Client").
- **`consensus-node-1/2/3`**: tre servizi quasi identici, ognuno con:
  - un `NODE_ID` diverso (`node-1`, `node-2`, `node-3`);
  - una porta interna diversa (`9001`/`9002`/`9003`) — non serve per forza che siano diverse dato che sono container separati, ma rende più leggibili i log e più facile il debug manuale (`docker exec` + `curl`/`grpcurl` su porte distinte se necessario);
  - **un volume dedicato e distinto per ciascun nodo** (`consensus-node-1-data`, ecc.), montato su `/data` — applica alla lettera il vincolo "Each consensus node must use an independent persistent volume for its on-disk log/state (never share a volume between consensus nodes)" (§11). I volumi sono dichiarati in fondo al file, sotto `volumes:`. Nota: gli stub attuali non scrivono ancora nulla su `/data` — sarà `internal/raft` a farlo, in Week 2+, quando verrà implementata la persistenza del log/`currentTerm`/`votedFor`.
  - `depends_on: [discovery]`: solo ordine di avvio (Compose non aspetta che discovery sia *pronto*, solo che il container sia partito) — un vero readiness gate arriverà eventualmente con un `healthcheck:` basato sull'endpoint gRPC health già implementato negli stub.
- **`snapshot-backup`**: un solo container, porta `8600`, nessuna porta pubblicata (non è mai un attore esterno).
- **`client-proxy`**: l'unico servizio con `ports: ["8080:8080"]`, cioè l'unico raggiungibile dall'host/esterno — esattamente il vincolo "the only... exposed externally" di §11.2.
- **`networks.b5-net`**: rete bridge dedicata e isolata su cui parlano tutti i servizi tra loro, coerente con "connected through a dedicated local Docker network (bridge network), not exposed externally" (§11.2).

Per **verificare** questo file (una volta disponibili Go/protoc/Docker): `make up` dovrebbe far partire tutti e sei i container, e ognuno deve loggare la propria riga "listening on :PORT" — a quel punto la connettività di rete è "verificata" nel senso richiesto dalla Settimana 1, anche senza alcuna logica applicativa reale.

---

## 9. `README.md`

Riassume lo stato del progetto (cosa è fatto vs. cosa manca), i prerequisiti di toolchain non disponibili in questo ambiente (Go, protoc + plugin, Docker) e i comandi `make` disponibili. È il punto di ingresso per chiunque (te stesso più avanti, o un collaboratore) clona il repo e vuole capire in due minuti a che punto è il progetto.

---

## 10. Cosa NON è stato fatto (di proposito) in questa fase

Per essere trasparenti sui limiti di questo scaffolding, coerentemente con l'ordine di sviluppo consigliato in §10.7:

- **Nessun codice `.pb.go` generato**: serve eseguire `protoc` localmente (non disponibile in questo ambiente sandbox).
- **Nessuna verifica di compilazione**: `go`, `protoc` e `docker` non sono installati qui, quindi né `go build`, né `make proto`, né `docker compose up` sono mai stati eseguiti realmente — solo scritti secondo la sintassi corretta.
- **`internal/raft`, `internal/statemachine`, `internal/proxy`, `internal/discovery`, `internal/circuitbreaker`, `internal/snapshot`**: cartelle ancora vuote, come da roadmap (logica di Raft, routing, breaker reali sono lavoro di Week 2+).
- **`experiments/` e `report/`**: fuori scope per la Settimana 1 (sono per gli scenari di valutazione, §12, e per la relazione finale, §15).

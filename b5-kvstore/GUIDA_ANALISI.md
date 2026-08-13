# Guida all'Analisi del Codice — b5-kvstore

> Guida di onboarding per chi analizza questo progetto per la prima volta.
> Segui i moduli **in ordine**: ogni modulo assume che tu abbia già letto
> quelli precedenti e apre solo i file necessari a quello step. Non
> anticipare: se un file non è ancora elencato, non ti serve ancora.

---

## 0. Prima di iniziare — stato reale del progetto

Questo non è un sistema completo: è un progetto didattico costruito per
settimane incrementali, e **oggi è fermo alla Week 2-3**. È fondamentale
saperlo prima di leggere il codice, altrimenti si rischia di cercare
componenti che semplicemente non esistono ancora:

| Componente | Stato | Dove |
|---|---|---|
| `internal/raft` (consenso: elezione, replica, persistenza) | **Implementato e testato** | `internal/raft/*.go` |
| `internal/raft/grpctransport` + `cmd/consensus-node` | **Implementato** (Raft reale su gRPC) | — |
| `internal/raft/harness` | **Implementato** (rete finta per i test) | — |
| `cmd/client-proxy` (API REST verso il client) | **Stub**: solo `/healthz` | `cmd/client-proxy/main.go` |
| `cmd/discovery` (service discovery) | **Stub** | `cmd/discovery/main.go` |
| `cmd/snapshot-backup` | **Stub** | `cmd/snapshot-backup/main.go` |
| `internal/statemachine`, `internal/proxy`, `internal/discovery`, `internal/snapshot`, `internal/circuitbreaker` | **Cartelle vuote**, non ancora scritte | — |

Di conseguenza questa guida si concentra quasi interamente su
`internal/raft/`: è l'unica parte del sistema in cui puoi effettivamente
studiare un algoritmo distribuito completo (Raft-lite, un sottoinsieme
semplificato del Raft del paper originale di Ongaro & Ousterhout). Gli
altri binari li vedremo solo per capire *dove si innesteranno* le parti
mancanti.

---

## 1. Introduzione & Mappa del Dominio

**Cosa fa il sistema.** b5-kvstore è (in prospettiva finale) un
key-value store replicato e fortemente consistente: più nodi mantengono
la stessa copia dei dati, e anche se uno o più nodi si guastano il
sistema continua a rispondere correttamente ai client, senza mai
mostrare dati diversi da nodo a nodo. Il nucleo che rende possibile
questa garanzia è **Raft**: un algoritmo di consenso distribuito che fa
in modo che un insieme di nodi si accordi, un comando alla volta, su
un'unica sequenza ordinata di operazioni (il "replicated log"), anche in
presenza di crash, riavvii e messaggi di rete persi o in ritardo.

**Quali problemi risolve.** Tre problemi distinti, che nel codice trovi
separati in moduli diversi. Primo, la *tolleranza ai guasti*: se il nodo
leader (quello che coordina le scritture) muore, il cluster deve
accorgersene ed eleggerne uno nuovo senza intervento umano, senza
"tempo morto" prolungato e senza corrompere lo stato replicato. Secondo,
il *consenso*: tutti i nodi devono applicare gli stessi comandi, nello
stesso ordine, anche quando ricevono messaggi duplicati, fuori ordine o
mai arrivati — è il problema che Raft (elezione + replica del log)
risolve esplicitamente. Terzo, la *persistenza e il recovery*: un nodo
che va giù e riparte non deve dimenticare cosa aveva già promesso
(voti dati, termini visti) né perdere comandi già confermati; deve poter
ricostruire il proprio stato da disco in modo affidabile anche se il
crash è avvenuto a metà di una scrittura.

**Panoramica dei componenti (senza codice, solo concetti).** Un
**Nodo** (`Node`) è un singolo membro del cluster: in ogni istante ha un
ruolo — Follower, Candidate o Leader — e questo ruolo cambia
dinamicamente in base a timeout ed elezioni, non è fissato all'avvio.
La **Rete/RPC** è il livello che permette a un nodo di parlare con gli
altri: nel progetto è astratta dietro un'interfaccia (`Transport`), in
modo che la stessa logica di consenso possa girare sia su una rete
finta in-process (per i test) sia su gRPC reale (tra processi/container
separati). Il **Log/Storage** è la sequenza ordinata e persistente dei
comandi accettati dal cluster: ogni nodo ne tiene una copia su disco,
scritta in modo da sopravvivere a un crash a metà scrittura. Il
**Motore di Consenso** è la logica che decide chi diventa leader, come
un'entry del log viene replicata e considerata "commit-tata" (cioè
definitivamente accettata dal cluster), e come si ripara un log
divergente dopo un cambio di leader.

---

## 2. Percorso di Lettura del Codice

### Modulo 1 — Strutture Dati e Stato Locale

**Obiettivo del modulo.** Capire *cosa* rappresenta un nodo Raft prima
di capire *come* si comporta. In questo step non seguiamo ancora nessun
flusso di esecuzione: leggiamo solo definizioni di tipo, per costruirci
un vocabolario mentale che useremo in tutti i moduli successivi.

**File coinvolti (solo questi):**
- `internal/raft/role.go`
- `internal/raft/persistence/persistence.go` — **solo** la parte
  dichiarativa in cima (tipo `LogEntry`, tipo `diskState`, costanti dei
  nomi di file), ignora per ora tutte le funzioni.
- `internal/raft/node.go` — **solo** i blocchi `TimingConfig`,
  `ApplyMsg`, `Config`, e la struct `Node` (righe ~36–174). Ignora tutte
  le funzioni sotto.

**Concetti chiave di Go & Sistemi Distribuiti.**
- *Enum idiomatico in Go*: `role.go` mostra il pattern standard —
  `type Role int` + costanti con `iota` + metodo `String()`. Go non ha
  enum nativi, questo è l'idioma con cui li si simula.
  Nota anche `toProto()`: converte il tipo Go interno nel tipo generato
  dal `.proto` — è il confine tra "modello interno" e "modello di rete",
  un pattern che rivedrai spesso in sistemi distribuiti (non esporre mai
  direttamente le struct interne sul wire).
- *Stato persistente vs stato volatile*: nella struct `Node` nota i
  commenti che dividono i campi in due gruppi — quelli che sopravvivono
  a un riavvio (`currentTerm`, `votedFor`, `log`) e quelli che vengono
  sempre ricalcolati da zero (`commitIndex`, `lastApplied`, `role`).
  Questa distinzione è **il concetto più importante di tutto il
  progetto**: tienila a mente, la ritroverai in ogni modulo successivo.
- *Log 1-indexed*: `LogEntry` e i commenti in `persistence.go` spiegano
  che l'indice 0 significa sempre "nessuna entry". È una convenzione
  presa dal paper Raft originale, non un dettaglio implementativo
  casuale.

**Guida passo-passo alla lettura.**
1. Apri `role.go` per primo: è il file più piccolo, ti dà il vocabolario
   (`Follower`, `Candidate`, `Leader`) che userai per leggere tutto il
   resto.
2. Passa a `persistence.go` e leggi solo `LogEntry` (riga ~21) e
   `diskState` (riga ~37). Chiediti: quali due informazioni bastano per
   ricordare "per chi/cosa ho già votato" dopo un riavvio? La risposta è
   in `diskState`.
3. Chiudi con la struct `Node` in `node.go`. Leggila **dall'alto in
   basso seguendo i commenti**, non i nomi dei campi: l'autore ha
   ordinato i campi esattamente per raccontare la storia "stato
   persistente → stato volatile → stato da leader → deadline
   dell'elezione". Prova a coprire i commenti e a indovinare a cosa
   serve ogni campo prima di rileggerli.

---

### Modulo 2 — Concorrenza e Thread-Safety

**Obiettivo del modulo.** Un nodo Raft è intrinsecamente concorrente:
riceve RPC da altri nodi (goroutine del server gRPC), controlla
periodicamente se è scaduto un timeout (goroutine dei ticker), e reagisce
a risposte RPC asincrone che arrivano in ordine imprevedibile. In questo
step impari **come il codice garantisce che tutto questo non corrompa lo
stato del nodo**.

**File coinvolti (solo questi):**
- `internal/raft/node.go` — questa volta le *funzioni*: `Start`, `Stop`,
  `electionTicker`, `heartbeatTicker`, `resetElectionDeadlineLocked`.
- Rileggi (solo per la concorrenza, non per il contenuto) le funzioni
  `startElection` e `broadcastAppendEntries` in `election.go` /
  `replication.go` — guarda **solo** dove appaiono `go func()`,
  `n.mu.Lock()` e `n.mu.Unlock()`, non ancora la logica Raft dentro.

**Concetti chiave di Go & Sistemi Distribuiti.**
- *Mutex e convenzione `xxxLocked`*: nota che moltissimi metodi hanno il
  suffisso `Locked` (es. `resetElectionDeadlineLocked`,
  `becomeLeaderLocked`). È una convenzione di naming (non imposta dal
  compilatore) che documenta un invariante: "questa funzione **presuppone**
  che `n.mu` sia già acquisito dal chiamante". È un modo leggero per
  evitare deadlock da doppio lock senza usare mutex rientranti (che Go
  non ha).
- *Il pattern "leggi sotto lock, rilascia, poi fai I/O di rete"*: guarda
  `startElection` — acquisisce il lock, legge/modifica lo stato, **rilascia
  il lock**, e solo dopo lancia le goroutine che fanno RPC di rete. Questo
  evita di tenere il mutex bloccato per la durata di una chiamata di rete
  (che può durare secondi), cosa che bloccherebbe anche i gestori RPC in
  arrivo. È un pattern fondamentale in Go per sistemi concorrenti: mai
  fare I/O bloccante con un mutex in mano.
- *Goroutine + channel per il ciclo di vita*: `Start()` lancia due
  goroutine permanenti (`electionTicker`, `heartbeatTicker`); `Stop()`
  chiude `stopCh`, e ogni ticker esce dal proprio `select` non appena
  quel channel si chiude. `sync.Once` garantisce che `close(stopCh)` non
  venga mai eseguito due volte (chiudere un channel già chiuso va in
  panic).
- *Race condition da "lock-then-async"*: nelle callback delle goroutine
  RPC (`election.go`/`replication.go`) nota i controlli tipo
  `if n.role != Candidate || n.currentTerm != term { return }` **dopo**
  aver ri-acquisito il lock. Chiediti: perché serve ricontrollare lo
  stato, se lo si è già controllato prima di lanciare la goroutine?

**Guida passo-passo alla lettura.**
1. `Start`/`Stop`/`stopCh`/`stopOnce`: capisci il ciclo di vita del nodo
   prima di tutto il resto.
2. `electionTicker` e `heartbeatTicker`: nota che sono quasi identici
   nella forma (loop + `select` su ticker e `stopCh`), ma diversi nella
   condizione (`role != Leader` vs `role == Leader`) — due facce della
   stessa moneta.
3. In `startElection`, segui il flusso: lock → mutazioni di stato →
   unlock → `for _, peer := range peers { go func() { ... } }`. Osserva
   che ogni goroutine lanciata riacquisisce il lock da sola quando la
   risposta RPC arriva.
4. Fai lo stesso in `broadcastAppendEntries`. Non preoccuparti ancora del
   *significato* Raft di quello che viene inviato — lo vedrai nel Modulo
   4. Concentrati solo sul pattern di concorrenza.

---

### Modulo 3 — Comunicazione di Rete & Messaggistica

**Obiettivo del modulo.** Capire come un nodo "parla" con gli altri, e
soprattutto capire **perché** questo progetto disaccoppia la logica Raft
dal trasporto effettivo — una scelta di design che permette di testare
scenari di guasto complessi senza mai avviare un container Docker.

**File coinvolti (solo questi):**
- `api/proto/consensus.proto` (il contratto — leggilo per primo)
- `internal/raft/transport.go` (l'interfaccia)
- `internal/raft/grpctransport/transport.go` (l'implementazione reale, gRPC)
- `internal/raft/harness/network.go` (l'implementazione finta, per i test)
- `cmd/consensus-node/main.go` (dove le due cose vengono cablate insieme
  in un binario eseguibile)

Non serve leggere `pkg/pb/*.go`: è codice **generato** da `protoc` a
partire dal `.proto`, non va letto riga per riga — sappi solo che esiste
e da dove viene.

**Concetti chiave di Go & Sistemi Distribuiti.**
- *Protocol Buffers come contratto di rete*: `.proto` definisce sia i
  messaggi (`RequestVoteRequest`, `AppendEntriesRequest`, ecc.) sia il
  servizio RPC (`service Consensus`). È il contratto condiviso tra tutti
  i nodi — cambiarlo significa cambiare il protocollo di rete di tutto
  il cluster.
- *Dependency Inversion via interfaccia*: `Transport` (`transport.go`)
  ha solo due metodi. `Node` dipende **solo** da questa interfaccia, mai
  da gRPC direttamente. Questo è ciò che rende possibile avere due
  implementazioni intercambiabili senza toccare una riga di logica Raft.
- *Context e timeout*: ogni chiamata RPC (sia reale che finta) accetta
  un `context.Context` con timeout (`defaultRPCTimeout`, Modulo 1). In
  un sistema distribuito una richiesta di rete **deve sempre** avere un
  limite di tempo esplicito — altrimenti un peer irraggiungibile blocca
  per sempre la goroutine chiamante.
- *Connection pooling*: `grpctransport.Transport` mantiene una mappa
  `conns map[string]*grpc.ClientConn`, riusando la stessa connessione
  verso lo stesso peer invece di aprirne una nuova ad ogni RPC.
- *Test double a livello di interfaccia, non di mock a oggetti*:
  `harness/network.go` non "finge" una connessione gRPC — implementa
  `raft.Transport` chiamando **direttamente** i metodi Go
  (`node.RequestVote(...)`) di un altro `Node` in-process, con la
  possibilità di inserire `Filter` che droppano o ritardano messaggi a
  piacere. Questo è ciò che rende i test di elezione/partizione veloci e
  deterministici invece che lenti e fragili (niente rete reale, niente
  Docker).

**Guida passo-passo alla lettura.**
1. `consensus.proto`: leggi la `service Consensus` e i quattro messaggi
   di richiesta/risposta principali (`RequestVote*`, `AppendEntries*`).
   Nota i campi `conflict_index`/`conflict_term` nella reply — li
   spiegherai bene solo nel Modulo 4, ma tienili a mente.
2. `transport.go`: due righe, un'interfaccia. È il fulcro concettuale di
   questo modulo — tutto il resto del modulo esiste per giustificare
   perché questa interfaccia è così piccola e così importante.
3. `grpctransport/transport.go`: segui `SendRequestVote` →
   `clientFor(addr)` → cache della connessione. Nota che è puro
   "plumbing": non c'è nessuna logica Raft qui, solo trasporto.
4. `harness/network.go`: leggi `fakeTransport.SendRequestVote` e il
   meccanismo `check()` → `Filter`. Confronta mentalmente con lo step
   precedente: stesso metodo dell'interfaccia, comportamento
   completamente diverso.
5. Chiudi con `cmd/consensus-node/main.go`: qui vedi il "wiring" finale
   — `grpctransport.New()` passato dentro `raft.Config{Transport: ...}`,
   il server gRPC creato con `pb.RegisterConsensusServer(srv, node)`
   (che funziona perché `Node` implementa i metodi richiesti da
   `pb.ConsensusServer` — lo vedrai concretamente nel Modulo 4).

---

### Modulo 4 — La Logica Distribuita Core

**Obiettivo del modulo.** Questo è il cuore del progetto: l'algoritmo
Raft-lite vero e proprio. Dopo questo modulo saprai rispondere a "come
viene eletto un leader" e "come viene replicato e confermato un comando
nel cluster", con tutte le regole di sicurezza che evitano stati
inconsistenti.

**File coinvolti (solo questi):**
- `internal/raft/election.go`
- `internal/raft/replication.go`

Rileggerai gli stessi metodi già "sfiorati" nel Modulo 2, ma ora
concentrandoti sul *significato*, non sulla concorrenza.

**Concetti chiave di Go & Sistemi Distribuiti.**
- *La regola universale del termine più alto* (`observeTermLocked` in
  `node.go`, richiamata sia da `election.go` che da `replication.go`):
  in Raft, un termine (`term`) è un contatore logico che identifica in
  modo univoco "l'epoca" di un'elezione. Qualunque nodo che veda un
  termine più alto del proprio deve immediatamente allinearsi e tornare
  Follower. È la regola singola più importante dell'intero algoritmo —
  nota che è centralizzata in **un solo posto** nel codice, chiamato da
  ogni RPC handler.
- *Maggioranza/quorum* (`quorumSize`, Modulo 1): sia l'elezione
  (`votes >= quorum`) sia il commit di un'entry (`maybeAdvanceCommitIndexLocked`)
  richiedono l'accordo della maggioranza dei nodi, non di tutti — questo
  è ciò che permette al cluster di continuare a funzionare anche con
  alcuni nodi giù.
- *Term-boundary commit rule* (il commento sopra
  `maybeAdvanceCommitIndexLocked`, che cita la "Figure 8" del paper
  Raft): una entry può essere considerata commit-tata **solo** se
  appartiene al termine corrente del leader — non basta che sia
  replicata su un quorum. Questa è la regola di sicurezza più
  controintuitiva di Raft: prova a capire perché serve prima di leggere
  la spiegazione che segue nel codice.
- *Fast log repair* (`ConflictIndex`/`ConflictTerm` in
  `handleAppendEntriesReplyLocked`, `firstIndexOfTermLocked`,
  `lastIndexOfTermLocked`): invece di far indietreggiare `nextIndex` di
  un'unità alla volta ad ogni rifiuto (lento se un follower è molto
  indietro), il follower comunica al leader "salta direttamente da qui"
  usando termine e indice del conflitto.
- *Write-ahead a livello di singolo nodo*: sia in `Propose` (leader) che
  in `AppendEntries` (follower) l'entry viene **prima** persistita su
  disco (`persistence.AppendLogEntries`, con `fsync`) e solo poi resa
  visibile nello stato in-memory / usata per rispondere all'RPC. Se
  l'ordine fosse invertito, un crash a metà potrebbe far credere al
  resto del cluster che un'entry sia durevole quando in realtà non lo è.

**Guida passo-passo alla lettura — segui esattamente questo ordine, non
l'ordine dei file:**
1. `RequestVote` in `election.go`: le due condizioni per concedere un
   voto (`notAlreadyVoted`, `candLogUpToDate` via
   `isLogAtLeastAsUpToDate`). Nota che il voto viene persistito
   (`persistStateLocked`) **prima** di rispondere.
2. `startElection` in `election.go`, questa volta leggendo il
   *contenuto*: incremento del termine, auto-voto, fan-out delle RPC,
   conteggio voti, `becomeLeaderLocked`.
3. `AppendEntries` in `replication.go`: è il metodo più denso del
   progetto. Segui l'ordine esatto delle verifiche come scritte nel
   codice: (a) regola del termine più alto, (b) rifiuto se termine
   troppo vecchio, (c) reset del timer di elezione, (d) controllo di
   consistenza su `prevLogIndex`/`prevLogTerm`, (e) `mergeEntriesLocked`,
   (f) avanzamento di `commitIndex` tramite `applyCommittedLocked`.
4. `Propose` in `replication.go`: il punto di ingresso di una scrittura
   lato leader (oggi chiamato solo da test/harness — sarà il Client
   Proxy a chiamarlo in una settimana futura).
5. `broadcastAppendEntries` + `handleAppendEntriesReplyLocked`: come il
   leader invia repliche/heartbeat e reagisce a successo o rifiuto.
6. `maybeAdvanceCommitIndexLocked`: leggila per ultima. È la funzione
   che raccoglie tutti i concetti precedenti — quorum, term-boundary
   rule — in poche righe dense.
7. (Facoltativo ma consigliato) `InstallSnapshot`/`GetStatus` in fondo a
   `replication.go`: nota che `InstallSnapshot` esiste solo per
   soddisfare l'interfaccia `pb.ConsensusServer`, non fa nulla di
   funzionale — coerente con quanto detto nella sezione 0 (snapshotting
   non ancora implementato).

---

### Modulo 5 — Persistenza e Ripristino

**Obiettivo del modulo.** Capire come un nodo sopravvive a un crash e a
un riavvio senza perdere garanzie, e senza corrompere il proprio stato
su disco anche se il crash avviene nel *momento peggiore possibile*
(a metà di una scrittura).

**File coinvolti (solo questi):**
- `internal/raft/persistence/persistence.go` (questa volta per intero,
  funzioni comprese — l'hai già visto solo nei tipi nel Modulo 1)
- `internal/raft/persistence/persistence_test.go` (leggi solo i nomi dei
  test e, per i 2-3 più interessanti, il corpo — non serve leggerli
  tutti)
- `internal/raft/node.go`, solo la funzione `NewNode` (il punto in cui
  lo stato persistito viene ricaricato all'avvio)

**Concetti chiave di Go & Sistemi Distribuiti.**
- *Write-to-temp-then-rename*: sia `SaveState` che `rewriteLog`
  scrivono prima su un file `.tmp` e poi chiamano `os.Rename`. Su
  filesystem POSIX, `rename` è atomico: o il file vecchio esiste ancora
  per intero, o esiste il nuovo per intero — mai uno stato a metà. È la
  tecnica standard per aggiornamenti "tutto o niente" su disco.
- *Length-prefixed records + fsync*: `log.dat` non è un semplice
  append di byte — ogni entry è preceduta da 4 byte che ne indicano la
  lunghezza (`encodeRecord`). Questo è ciò che rende possibile
  distinguere, alla lettura, un record completo da uno scritto solo a
  metà da un crash (`ReadAllLogEntries`, i tre casi di `err` gestiti:
  EOF pulito, prefisso di lunghezza tagliato, payload tagliato). Nota
  anche `f.Sync()` alla fine di `AppendLogEntries`: senza questa
  chiamata esplicita, il sistema operativo potrebbe tenere i dati solo
  in cache e perderli in caso di crash, anche se `Write` è già
  ritornato con successo.
- *Trade-off esplicito e documentato*: leggi con attenzione il commento
  sopra `SaveState` (righe ~60-69) — a differenza del log,
  `state.json` **non** viene fsync-ato in modo sincrono prima di
  rispondere all'RPC. È un compromesso deliberato tra sicurezza e
  costo, con le conseguenze esplicitamente accettate per iscritto nel
  codice. In un sistema reale ogni scelta di persistenza dovrebbe avere
  questo tipo di giustificazione esplicita.
- *Recovery come parte del percorso normale, non un caso speciale*:
  nota che `ReadAllLogEntries` non lancia mai un errore per un record
  finale incompleto — tronca silenziosamente e ritorna quello che è
  riuscita a leggere. Questo perché per Raft "l'ultima entry scritta ma
  non confermata da un crash" è indistinguibile da "l'entry non è mai
  stata scritta" — ed è esattamente il comportamento corretto da un
  punto di vista del protocollo.

**Guida passo-passo alla lettura.**
1. `LoadState`/`SaveState`: nota l'asimmetria — `LoadState` tratta un
   file mancante come "nodo nuovo", non come errore.
2. `AppendLogEntries`/`ReadAllLogEntries`: leggili in coppia, sono
   scritti (dallo stesso encoding) uno per essere l'inverso dell'altro.
   Prova a immaginare un crash a metà della scrittura del quarto byte
   del prefisso di lunghezza, e segui manualmente quale `if` in
   `ReadAllLogEntries` lo intercetta.
3. `TruncateLogFrom`/`rewriteLog`: il percorso usato dal *fast log
   repair* del Modulo 4 quando un leader deve sovrascrivere entry
   sbagliate di un follower. Nota che riusa `ReadAllLogEntries` invece
   di riscrivere la logica di parsing da zero.
4. Torna a `node.go` → `NewNode`: ora che conosci `LoadState` e
   `ReadAllLogEntries`, leggi come i loro risultati popolano la nuova
   struct `Node`, e nota il commento su *perché* il nodo parte sempre
   come Follower indipendentemente da cosa dice `state.json`.
5. Apri `persistence_test.go` e cerca i test con "crash", "torn" o
   "partial" nel nome: sono la controprova pratica di quanto hai appena
   letto — mostrano esattamente lo scenario di crash-a-metà-scrittura
   simulato e verificato.

---

## 3. Checklist di Tracciamento del Flusso (Life of a Request)

### A. Elezione di un nuovo leader

```
[electionTicker rileva timeout scaduto, ruolo != Leader]
        │  node.go: electionTicker()
        ▼
startElection()                              election.go
        │  incrementa currentTerm, vota per sé, persiste su disco
        │  (persistence.SaveState — NON fsync sincrono, trade-off accettato)
        ▼
Fan-out RequestVote a tutti i peer, in goroutine parallele
        │  via Transport.SendRequestVote()                 transport.go
        │  ├─ implementazione reale → grpctransport (gRPC su rete)
        │  └─ implementazione test  → harness/network.go (in-process, con Filter)
        ▼
Ogni peer riceve RequestVote()                              election.go
        │  applica la regola del termine più alto
        │  verifica "non ho già votato per un altro" + "il suo log è aggiornato almeno quanto il mio"
        │  concede o nega il voto, persiste il voto se concesso
        ▼
Il candidato conta le risposte positive man mano che arrivano
        │  al raggiungimento del quorum → becomeLeaderLocked()      election.go
        ▼
Il nuovo leader inizializza nextIndex/matchIndex per ogni peer
        │  e invia subito un primo AppendEntries (asserzione di leadership)
        ▼
[i follower vedono AppendEntries dal nuovo leader → resettano il proprio timer di elezione]
```

### B. Una scrittura accettata dal cluster (comando → commit → apply)

> Oggi `Propose` è invocato direttamente da test/harness. In futuro sarà
> il Client Proxy (non ancora implementato) a chiamarlo dopo aver
> ricevuto una richiesta REST dal client esterno — tienilo a mente
> mentre leggi il flusso.

```
Propose(command) chiamato sul nodo che crede di essere Leader     replication.go
        │  se non è Leader → ritorna isLeader=false (nessun side effect)
        │  altrimenti: costruisce la LogEntry, la scrive su disco
        │  PRIMA (persistence.AppendLogEntries, fsync sincrono),
        │  poi la aggiunge al log in memoria
        ▼
broadcastAppendEntries()                                          replication.go
        │  per ogni peer costruisce un AppendEntriesRequest
        │  (prevLogIndex/prevLogTerm per il check di consistenza,
        │  le entry mancanti, leaderCommit)
        ▼
Ogni follower riceve AppendEntries()                              replication.go
        │  regola del termine più alto → eventuale passo a Follower
        │  controllo di consistenza su prevLogIndex/prevLogTerm
        │    ├─ inconsistente → risponde Success=false + ConflictIndex/ConflictTerm
        │    └─ consistente   → mergeEntriesLocked (tronca eventuali conflitti,
        │                       APPENDE E FSYNCA su disco, poi in memoria)
        │  se leaderCommit > commitIndex locale → applyCommittedLocked()
        ▼
Il leader riceve le risposte (handleAppendEntriesReplyLocked)     replication.go
        │  ├─ successo  → aggiorna matchIndex/nextIndex per quel peer
        │  └─ rifiuto   → nextIndex fatto arretrare via fast-backoff
        │                 (conflictIndex/conflictTerm), riproverà al prossimo giro
        ▼
maybeAdvanceCommitIndexLocked()                                   replication.go
        │  cerca il più alto N replicato su un quorum
        │  E appartenente al currentTerm del leader (term-boundary rule)
        │  → se trovato, commitIndex = N
        ▼
applyCommittedLocked() su leader (subito) e su ogni follower
(al prossimo AppendEntries/heartbeat che porta leaderCommit aggiornato)
        │  invoca ApplyFn per ogni entry appena commit-tata, in ordine
        ▼
[oggi: ApplyFn è solo un log line (cmd/consensus-node) o bookkeeping di test (harness)
 — nessuna risposta torna al client, perché il Client Proxy non esiste ancora]
```

Prova a rifare tu, per iscritto, la stessa traccia per uno **heartbeat
puro** (nessuna entry nuova, `broadcastAppendEntries` con `entries`
vuoto): quali passaggi del flusso B restano identici e quali si
saltano?

---

## 4. Domande di Autoverifica

Non cercare le risposte online: sono tutte derivabili rileggendo i file
già visti. Se non sai rispondere con sicurezza a una di queste, torna al
modulo corrispondente prima di andare avanti.

1. **[Modulo 4]** In `AppendEntries`, cosa succede esattamente se
   `req.GetTerm() < n.currentTerm`? Perché il nodo risponde comunque con
   il proprio `currentTerm` invece di ignorare semplicemente la
   richiesta?

2. **[Modulo 2 + 4]** In `startElection`, perché la funzione controlla
   `if n.role == Leader { return }` proprio all'inizio, prima di
   incrementare il termine? Cosa succederebbe di sbagliato — e a chi —
   se un leader legittimo, per un bug, chiamasse comunque
   `startElection`?

3. **[Modulo 4]** La regola del "term-boundary commit"
   (`maybeAdvanceCommitIndexLocked` committa solo entry del
   `currentTerm` del leader) è una delle parti più difficili di Raft.
   Prova a costruire uno scenario concreto — con numeri di termine e
   indici — in cui, **senza** questa regola, un'entry già replicata su
   un quorum potrebbe essere sovrascritta da un leader successivo.

4. **[Modulo 3 + 5]** Il `Filter` in `harness/network.go` permette di
   far arrivare una `AppendEntries` a un follower (che quindi la
   persiste regolarmente su disco) ma di far *perdere* la risposta al
   leader (`DropReplyFrom`). Perché questo scenario è interessante da
   testare — cosa "sa" il leader in questo caso, e cosa succede se
   proprio in quel momento il leader crasha?

5. **[Modulo 5]** Il commento sopra `SaveState` accetta esplicitamente
   il rischio di un doppio voto dopo un crash proprio a cavallo di un
   cambio di termine, perché evitarlo richiederebbe `fsync` ad ogni
   voto. Sei d'accordo con questo trade-off? In quali condizioni
   operative (es. cluster molto grande, rete molto instabile) lo
   ridiscuteresti?

6. **[Modulo 1 + 4]** `commitIndex` e `lastApplied` sono deliberatamente
   **non** persistiti su disco, e vengono ricalcolati da zero (partendo
   da 0) ad ogni riavvio. Perché è sicuro farlo? Cosa garantisce che un
   nodo riavviato non "dimentichi" temporaneamente qualcosa che il
   cluster aveva già confermato come commit-tato, in un modo che
   comprometta la consistenza?

---

*Una volta completati tutti i moduli e risposto (onestamente, senza
rileggere) alle domande della sezione 4, sei pronto a leggere
`internal/raft/raft_test.go` e
`internal/raft/harness/scenarios_test.go` per intero: sono la prova
end-to-end di tutti i comportamenti descritti in questa guida, e ti
diranno immediatamente se la tua comprensione ha delle lacune.*

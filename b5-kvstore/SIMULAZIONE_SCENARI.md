# Simulazione di Scenari — b5-kvstore (Raft-lite)

Questo documento traccia, passo per passo e con numeri concreti (term,
indici di log, commitIndex), cinque scenari di esecuzione del motore di
consenso implementato in `internal/raft/`. Ogni passaggio è ancorato a
funzioni/campi reali del codice (`election.go`, `replication.go`,
`persistence.go`) — non è pseudocodice generico, è il comportamento
effettivo di questa implementazione.

## Legenda e notazione

- **Nodo**: `N1`, `N2`, ... (in scenari a 3 nodi: `N1..N3`; a 5 nodi:
  `N1..N5`).
- **Entry di log**: notata come `idx(term)`, es. `idx4(term2)` = entry
  all'indice 4, scritta durante il termine 2. Il log è 1-indexed
  (`persistence.LogEntry`, indice 0 = "nessuna entry").
- **Quorum**: `quorumSize(n) = n/2 + 1` (`node.go`). Con 3 nodi: 2. Con
  5 nodi: 3.
- **Stato di un nodo**: Ruolo (F=Follower, C=Candidate, L=Leader),
  `currentTerm`, `votedFor`, log, `commitIndex`.
- **Campi RPC richiamati** (da `api/proto/consensus.proto` e
  `pb.*Request/Reply`):
  - `RequestVote{Term, CandidateId, LastLogIndex, LastLogTerm}` →
    `{Term, VoteGranted}`
  - `AppendEntries{Term, LeaderId, PrevLogIndex, PrevLogTerm, Entries,
    LeaderCommit}` → `{Term, Success, ConflictIndex, ConflictTerm}`

---

## Scenario 1 — Elezione senza guasti (baseline)

**Cluster**: 3 nodi (`N1, N2, N3`), quorum = 2. Stato iniziale: tutti
Follower, `term=0`, `votedFor=""`, log vuoto, `commitIndex=0`.

| Passo | Evento | Dettaglio |
|---|---|---|
| 1 | `N2.electionTicker` scade per primo (timeout randomizzato più corto) | `startElection()`: `currentTerm` 0→1, `votedFor=N2`, persistito (`SaveState`), `resetElectionDeadlineLocked()`. `lastLogIndex=0, lastLogTerm=0`. `votes=1` (self), quorum=2 → non ancora raggiunto. |
| 2 | N2 invia `RequestVote{Term:1, CandidateId:N2, LastLogIndex:0, LastLogTerm:0}` a N1 e N3, in parallelo (goroutine separate) | — |
| 3 | N1 riceve la richiesta | `req.Term(1) > currentTerm(0)` → `observeTermLocked`: `currentTerm=1`, `votedFor=""`, torna Follower, persiste. `notAlreadyVoted` (votedFor=="") = true. `candLogUpToDate`: `candTerm(0)==voterTerm(0)`, `candIndex(0)>=voterIndex(0)` = true. **Vota N2**, persiste, resetta il proprio timer. Reply `{Term:1, VoteGranted:true}`. |
| 4 | N3 riceve la stessa richiesta | Simmetrico a N1 → vota N2. Reply `{Term:1, VoteGranted:true}`. |
| 5 | N2 riceve la reply di N1 | `votes=2 >= quorum(2)` → `becomeLeaderLocked()`: role=Leader, `nextIndex={N1:1,N3:1}`, `matchIndex={N1:0,N3:0}`. Lancia subito `broadcastAppendEntries()` (asserzione immediata di leadership). |
| 6 | N2 invia `AppendEntries{Term:1, PrevLogIndex:0, PrevLogTerm:0, Entries:[], LeaderCommit:0}` a N1, N3 | Heartbeat puro, nessuna entry. |
| 7 | N1, N3 ricevono l'heartbeat | `term(1)==currentTerm(1)` → Follower (già lo erano), `currentLeader=N2`, timer resettato. `PrevLogIndex=0` → check di consistenza saltato. Reply `{Term:1, Success:true}`. |
| 8 | La reply di voto di N3 (passo 4) arriva a N2 *dopo* che è già Leader | In `startElection`, il controllo `if n.role != Candidate ... return` scarta la reply: nessun effetto, nessun errore. |

**Stato finale:**

| Nodo | Ruolo | Term | VotedFor | Log | CommitIndex |
|---|---|---|---|---|---|
| N1 | Follower | 1 | N2 | — | 0 |
| N2 | **Leader** | 1 | N2 | — | 0 |
| N3 | Follower | 1 | N2 | — | 0 |

---

## Scenario 2 — Split vote e risoluzione senza nuovo termine

**Cluster**: 3 nodi, quorum = 2. Stato iniziale identico allo
Scenario 1 (reset).

| Passo | Evento | Dettaglio |
|---|---|---|
| 1 | I timer di N1 e N3 scadono quasi in contemporanea (jitter ravvicinato) | Entrambi eseguono `startElection()` indipendentemente: N1 → `term=1, votedFor=N1`; N3 → `term=1, votedFor=N3`. Nessuna race sui dati: ogni nodo modifica solo il proprio stato sotto il proprio lock. |
| 2 | N2 riceve prima la richiesta di N1 | `term(1)>currentTerm(0)` → allineamento, poi `notAlreadyVoted=true` → **vota N1**. `votedFor=N1`. |
| 3 | N2 riceve poi la richiesta di N3 (stesso term=1) | `notAlreadyVoted`: `votedFor("N1") != candidateId("N3")` → **false** → **rifiuta**. Reply `{Term:1, VoteGranted:false}`. |
| 4 | N1 riceve la richiesta di voto di N3 (N1 è a sua volta Candidate) | `term(1)==N1.currentTerm(1)`. `notAlreadyVoted`: `votedFor("N1") != "N3"` → false → **rifiuta**. |
| 5 | N1 conta i voti: self + N2 = 2 ≥ quorum(2) | `becomeLeaderLocked()` → invia subito `AppendEntries{Term:1, ...}` a N2, N3. |
| 6 | N3 (ancora Candidate in term=1) riceve questo AppendEntries da N1 | `term(1) == N3.currentTerm(1)` → il ramo "term uguale" viene eseguito **incondizionatamente**: `n.setRoleLocked(Follower)`. N3 torna Follower **immediatamente**, senza aspettare la scadenza del proprio election timeout e senza incrementare ulteriormente il termine. |

**Punto notevole**: lo split vote qui si risolve **nello stesso
termine** (term=1), non richiede un round successivo con nuovo timeout
randomizzato — il candidato "perdente" si arrende non appena riceve la
prova (un `AppendEntries` valido) che qualcun altro ha già vinto.

**Stato finale:**

| Nodo | Ruolo | Term | VotedFor |
|---|---|---|---|
| N1 | **Leader** | 1 | N1 |
| N2 | Follower | 1 | N1 |
| N3 | Follower | 1 | N3 *(mai riscritto — irrilevante da Follower)* |

---

## Scenario 3 — Crash del leader e Fast Log Repair (`ConflictIndex`/`ConflictTerm`)

**Cluster**: 3 nodi, quorum = 2.

**Stato pre-scenario** (dopo una storia già stabile, non dettagliata):

| Nodo | Log | CommitIndex |
|---|---|---|
| N1 (leader, term=2) | `idx1(1) idx2(1) idx3(1) idx4(2) idx5(2)` | 3 |
| N2 | `idx1(1) idx2(1) idx3(1)` | 3 |
| N3 | `idx1(1) idx2(1) idx3(1)` | 3 |

`idx4(2)` e `idx5(2)` sono stati scritti da N1 tramite `Propose()`
(quindi già fsyncati sul *suo* disco) ma **mai replicati con successo a
nessuno**: N1 crasha subito dopo averli scritti localmente.

| Passo | Evento | Dettaglio |
|---|---|---|
| 1 | N1 giù. N2 vince l'elezione per `term=3` | Log di N2 (`idx3/term1`) pari a quello di N3 → N3 vota N2. `becomeLeaderLocked`: `nextIndex={N1:4, N3:4}`. |
| 2 | Client chiama `Propose()` su N2 | Nuova entry `idx4(term3)`, fsyncata su N2, poi replicata. |
| 3 | N2 → N3: `PrevLogIndex=3, PrevLogTerm=1` | `N3.entryAt(3)` ha `term=1` = coerente → append `idx4(3)`. `Success=true`. `matchIndex[N3]=4`. |
| 4 | N1 si riavvia | `NewNode` ricarica da disco: `currentTerm=2, votedFor=N1, log=[idx1..5]`. Ruolo sempre **Follower** all'avvio, indipendentemente da cosa dice `state.json` (`node.go`, `NewNode`). |
| 5 | N2 (leader term=3) contatta N1. `nextIndex[N1]` era rimasto ottimisticamente a 5 (inizializzato a `lastIdx+1` al momento dell'elezione, mai aggiornato mentre N1 era irraggiungibile) | `PrevLogIndex=4, PrevLogTerm=3` (l'entry `idx4` di N2 ha term 3). |
| 6 | N1 verifica `entryAtLocked(4)` | Esiste, ma `term=2 ≠ PrevLogTerm(3)` → **conflitto di termine**. `conflictTerm := entry.Term = 2`. `ConflictIndex = firstIndexOfTermLocked(2)`: scansione all'indietro sul log di N1 — `idx5=term2, idx4=term2, idx3=term1` (diverso, stop) → **ConflictIndex = 4**. Reply `{Success:false, ConflictTerm:2, ConflictIndex:4}`. |
| 7 | N2 riceve la reply | `ConflictTerm(2) ≠ 0` → cerca `lastIndexOfTermLocked(2)` nel **proprio** log: N2 non ha mai avuto alcuna entry con `term=2` → non trovata → `next = reply.ConflictIndex = 4`. `nextIndex[N1] := 4`. |
| 8 | Retry: N2 → N1, `PrevLogIndex=3, PrevLogTerm=1` | `N1.entryAt(3)=term1` → coerente. N2 invia `idx4(term3)`. |
| 9 | `mergeEntriesLocked` su N1 | `insertAt=4`. `N1.entryAt(4)` esiste con `term=2 ≠` term in arrivo (3) → **`TruncateLogFrom(dir, 4)`**: N1 scarta `idx4, idx5(term2)` (dati orfani, mai committed da nessuno) sia su disco che in memoria. Poi appende `idx4(term3)`. `Success=true`. |

**Perché conta il fast-backoff**: senza `ConflictIndex/ConflictTerm`,
il leader avrebbe dovuto decrementare `nextIndex[N1]` di un'unità alla
volta (5→4→3...), un RPC di andata-ritorno per ogni indice divergente.
Con il salto diretto a `ConflictIndex=4`, un'unica RPC in più basta,
indipendentemente da quante entry orfane N1 avesse accumulato.

**Stato finale:** N1, N2, N3 tutti allineati su
`idx1(1) idx2(1) idx3(1) idx4(3)`.

---

## Scenario 4 — Term-Boundary Commit Rule (il caso "Figure 8")

**Cluster**: 5 nodi (`N1..N5`), quorum = 3. Serve un cluster a 5 nodi
perché lo scenario richiede una minoranza di 2 sopravvissuta a un primo
crash e una maggioranza di 3 che rielegge in modo indipendente.

**Stato pre-scenario**: tutti i nodi hanno `idx1(term1)`, committed,
`commitIndex=1`.

| Passo | Evento | Dettaglio |
|---|---|---|
| 1 | N1 diventa Leader, `term=2`. Propone `idx2(term2)` | Replicata con successo **solo a N2** (`matchIndex[N2]=2`) prima che N1 crashi. `count` per idx2 = N1(leader)+N2 = 2 nodi su 5 < quorum(3) → **mai committed**. N3, N4, N5 non l'hanno mai vista. |
| 2 | N1 giù. Elezione `term=3` | N4 si candida con `lastLogIndex=1, lastLogTerm=1` (uguale a N3, N5). Riceve voti da N3, N5 (+self) = 3 ≥ quorum → **N4 diventa Leader, term=3**, ignaro di `idx2(term2)`. |
| 3 | N4 manda un heartbeat a N2: `PrevLogIndex=1, PrevLogTerm=1` | Coerente (N2 ha comunque `idx1/term1`). Nessuna entry inviata in questo passo → `idx2(term2)` locale di N2 **non viene toccato**. |
| 4 | N4 crasha subito dopo, senza aver mai proposto una propria entry | — |
| 5 | Elezione `term=4`. N1 si è ripreso, `lastLogIndex=2, lastLogTerm=2` — **più aggiornato** di N3/N5 (`lastLogIndex=1, term=1`) | N3, N5 votano N1 (log meno aggiornato del candidato) → **N1 torna Leader, term=4**. `nextIndex` iniziali = 3 per ogni peer. |
| 6 | N1 → N3: `PrevLogIndex=2, PrevLogTerm=2` | `N3.entryAt(2)` non esiste (N3 fermo a idx1) → Case A: `Success=false, ConflictIndex=lastLogIndex(N3)+1=2, ConflictTerm=0`. N1: `ConflictTerm=0` → `next=ConflictIndex=2`. Retry con `PrevLogIndex=1,PrevLogTerm=1` → coerente → N1 invia `idx2(term2)` → N3 lo accetta. `matchIndex[N3]=2`. Stesso esito per N5. |
| 7 | **Punto cruciale**: ora `matchIndex` per `idx2` è: N2=2 (mai perso), N3=2, N5=2, + N1 stesso = **4 nodi su 5** con `idx2` — ben oltre il quorum(3) | `maybeAdvanceCommitIndexLocked` scorre da `N=lastLogIndex` verso il basso: trova `idx2`, ma `entry.Term(2) != n.currentTerm(4)` → **`continue`**, la entry viene scartata dal controllo nonostante il quorum numerico. **`commitIndex` resta a 1.** |
| 8 | *(Ipotetico, se la regola non esistesse)*: `idx2` verrebbe marcato committed qui, in `term=4`, nonostante porti `term=2` | Rischio concreto: un futuro leader (es. un ipotetico N5 che vincesse un'elezione successiva con una propria entry mai vista dagli altri all'indice 2) potrebbe **sovrascrivere** `idx2` — violando la garanzia "un'entry committed non cambia mai più", su cui ogni client che ha ricevuto conferma di scrittura fa affidamento. |
| 9 | N1 esegue `Propose()` di una **propria** entry: `idx3(term4)`. Raggiunge il quorum (`matchIndex ≥ 3` su almeno 3 nodi) | `maybeAdvanceCommitIndexLocked`: `N=3`, `entry.Term(4)==currentTerm(4)` → idoneo, **commit diretto di `idx3`**. Il ciclo si ferma al primo `N` idoneo scorrendo dall'alto, quindi `commitIndex` passa **direttamente a 3** — portando con sé, indirettamente, anche `idx2` (mai committed da solo, ora "coperto" dal commit di `idx3`). |

Questo è esattamente il comportamento descritto nel commento del
codice sopra `maybeAdvanceCommitIndexLocked`: *"Older-term entries are
committed only indirectly, once covered by a same-term N."*

---

## Scenario 5 — Partizione di rete (leader in minoranza)

**Cluster**: 5 nodi, quorum = 3.

**Stato pre-scenario**: N1 Leader, `term=2`, tutti i nodi allineati su
`idx1..idx3(term2)`, `commitIndex=3`.

| Passo | Evento | Dettaglio |
|---|---|---|
| 1 | Partizione di rete: `{N1, N2}` isolati da `{N3, N4, N5}` — nessun messaggio passa in nessuna direzione tra i due gruppi | — |
| 2 | Il client continua a chiamare `Propose()` su N1 (che localmente si crede ancora Leader — nessuno gli ha detto il contrario) | N1 appende `idx4(2), idx5(2)`, fsyncati localmente, replicati con successo **solo a N2** (`matchIndex[N2]=5`). Verso N3/N4/N5 ogni RPC va in timeout (`defaultRPCTimeout`, 2s). `maybeAdvanceCommitIndexLocked`: quorum disponibile = N1+N2 = 2 su 5 < 3 → **`commitIndex` resta bloccato a 3**, indefinitamente, per tutta la durata della partizione. `idx4, idx5` restano scritti ma mai confermati. |
| 3 | Lato maggioranza `{N3,N4,N5}`: dopo il timeout di elezione (nessun heartbeat da N1 arriva più), N4 avvia un'elezione | `currentTerm` 2→3, `lastLogIndex=3, lastLogTerm=2` (comune a N3, N5). Riceve i loro voti (+self) = 3 ≥ quorum → **N4 diventa Leader, term=3**, senza mai sapere nulla di `idx4/idx5` di N1 — giustamente, non erano mai stati committed. |
| 4 | *(Concettuale, il Client Proxy non esiste ancora in questa fase del progetto)*: un client che scrivesse ora tramite N4 otterrebbe un vero commit | `Propose(idx4', term3)` replicato su N3, N4, N5 → quorum 3/5 → **committed regolarmente**. La vecchia `idx4(term2)` di N1 resta orfana sulla minoranza. |
| 5 | La partizione si richiude. N1 (ancora convinto di essere Leader, term=2) manda un heartbeat a N3 | `N3.currentTerm(3) > req.Term(2)` → N3 rifiuta: `{Term:3, Success:false}`. |
| 6 | N1 riceve questa reply in `handleAppendEntriesReplyLocked` | `reply.Term(3) > n.currentTerm(2)` → `observeTermLocked`: `currentTerm=3, votedFor="", role=Follower`. N1 smette immediatamente di comportarsi da leader. |
| 7 | N2 riceve, prima o poi, un `AppendEntries` dal vero leader N4 | `term(3) > N2.currentTerm(2)` → allineamento a `term=3`, Follower. |
| 8 | N4 manda `AppendEntries` a N1 e N2 con `PrevLogIndex/PrevLogTerm` che puntano alla propria `idx4'(term3)` | Diverge dalle `idx4(term2)` locali di N1/N2 → `mergeEntriesLocked` le **tronca** e le sostituisce con la storia della maggioranza. |

**Esito**: le scritture `idx4, idx5(term2)` fatte da N1 durante
l'isolamento **spariscono senza essere mai state confermate a nessun
client** — comportamento corretto: non avendo mai raggiunto un quorum,
non erano mai state "committed", quindi scartarle non viola nessuna
promessa del sistema. È precisamente per questo che, in Raft, il
"commit" è un evento che conta solo se confermato da un **quorum**, mai
dalla sola convinzione di un nodo di essere leader.

---

## Riepilogo — cosa dimostra ciascuno scenario

| Scenario | Meccanismo del codice messo alla prova |
|---|---|
| 1 — Elezione baseline | `startElection`, `RequestVote`, `becomeLeaderLocked` |
| 2 — Split vote | `notAlreadyVoted`, ramo incondizionato "term uguale → Follower" in `AppendEntries` |
| 3 — Fast log repair | `ConflictIndex`/`ConflictTerm`, `firstIndexOfTermLocked`, `lastIndexOfTermLocked`, `TruncateLogFrom` |
| 4 — Term-boundary commit | `maybeAdvanceCommitIndexLocked`, la guardia `entry.Term != n.currentTerm` |
| 5 — Partizione di rete | Impossibilità di commit in minoranza, `observeTermLocked` al ripristino, sicurezza garantita da "mai committed senza quorum" |

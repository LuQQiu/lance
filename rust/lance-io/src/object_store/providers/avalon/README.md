# Avalon - Distributed Caching Layer for Lance

Avalon is a distributed caching layer that sits between Lance and object storage backends (S3, Azure, GCS, local filesystem). It provides a transparent read-through cache to accelerate data access for ML workloads.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Lance Application                           │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     AvalonObjectStore                               │
│   (Implements object_store::ObjectStore trait)                      │
│                                                                     │
│   - Intercepts read operations (get, get_opts, get_range)           │
│   - Routes requests through Avalon cluster                          │
│   - Falls back to inner store on errors                             │
└─────────────────────────────────────────────────────────────────────┘
                                    │
              ┌─────────────────────┴─────────────────────┐
              ▼                                           ▼
┌───────────────────────────┐               ┌───────────────────────────┐
│    Avalon Cluster         │               │    Inner Object Store     │
│                           │               │    (S3, Azure, GCS, etc.) │
│   ┌─────┐ ┌─────┐ ┌─────┐│               │                           │
│   │Node1│ │Node2│ │Node3││               │   (Writes go directly     │
│   └─────┘ └─────┘ └─────┘│               │    to this store)         │
│                           │               │                           │
│   Distributed cache nodes │               │                           │
│   with consistent hashing │               │                           │
└───────────────────────────┘               └───────────────────────────┘
```

## Components

### 1. Protocol Definition (`protos/avalon.proto`)

Defines the gRPC service interface:

- **FetchChunk**: Fetches a chunk of data from a cached object
  - Request: `prefix`, `path`, `offset`, `length`
  - Response: `object_size`, `object_mtime`, `payload`

- **GetCluster**: Returns the current cluster topology
  - Response includes: `cluster_id`, `client_chunk_size`, `source_node_id`, `nodes[]`

- **RemoveNode**: Permanently removes a node from the cluster

### 2. Provider (`provider.rs`)

The `AvalonObjectStoreProvider` implements Lance's `ObjectStoreProvider` trait:

```rust
pub struct AvalonObjectStoreProvider;

impl ObjectStoreProvider for AvalonObjectStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore>;
}
```

**URL Scheme**: `avalon://`

The URL format encodes the inner storage scheme:
- `avalon://s3_mybucket/path` → `s3://mybucket/path`
- `avalon://az_container@account/path` → `az://container@account/path`
- `avalon://file-object-store/tmp/data` → `file-object-store:///tmp/data`

**Configuration**:
- Avalon endpoints can be specified via:
  - Storage option: `avalon_endpoints`
  - Environment variable: `AVALON_ENDPOINTS`
- Format: comma-separated `host:port` pairs (e.g., `localhost:9090,localhost:9091,localhost:9092`)

### 3. Object Store (`object_store.rs`)

The `AvalonObjectStore` wraps an inner object store and intercepts read operations:

```rust
pub struct AvalonObjectStore {
    pub inner: ObjectStore,        // The underlying storage (S3, Azure, etc.)
    pub inner_prefix: String,      // Unique prefix for cache key generation
    pub clients: Arc<dyn AvalonClients>,  // gRPC client pool
    pub fallback_reads: AtomicU64, // Counter for fallback operations
}
```

**Read Path**:
1. Client calls `get_opts(location, options)`
2. Avalon computes which cluster node owns the chunk (via consistent hashing)
3. `FetchChunk` RPC is sent to the appropriate node
4. Node returns cached data (or fetches from backing store on cache miss)
5. Data is streamed back to the client

**Fallback Behavior**:
- On Avalon errors, reads fall back to the inner object store directly
- Fallback counter tracks these events (logged every 1000 occurrences)
- Suffix range requests always fall back (not supported by Avalon protocol)

**Write Path**:
- All write operations (`put`, `put_opts`, `put_multipart`, etc.) pass directly to the inner store
- Avalon is a read-through cache only

### 4. Client Management (`clients.rs`)

The `AvalonClientsImpl` manages connections to cluster nodes:

```rust
pub struct AvalonClientsImpl {
    cluster_data: Mutex<ClusterData>,
}

struct ClusterData {
    bootstrap_endpoints: Vec<AvalonEndpoint>,  // Initial endpoints for discovery
    next_bootstrap_endpoint: usize,            // Round-robin index
    cluster: Option<Arc<AvalonClusterAndHasher>>,  // Cached topology
    node_clients: HashMap<u32, Arc<Mutex<AvalonNodeClient>>>,  // Per-node clients
}
```

**Cluster Discovery**:
1. On first request, client connects to a bootstrap endpoint
2. `GetCluster` RPC retrieves the full cluster topology
3. Topology is cached and used for subsequent routing decisions
4. Client connections to nodes are established lazily

**Connection Handling**:
- Uses tonic/gRPC for communication
- 10-second connection timeout
- 1 GiB max message size for large chunk transfers

### 5. Cluster Topology (`cluster.rs`)

The `AvalonCluster` represents the cluster state:

```rust
pub struct AvalonCluster {
    pub cluster_id: Uuid,              // Unique cluster identifier
    pub client_chunk_size: u32,        // Chunk size for hashing (e.g., 16KB)
    pub source_node_id: u32,           // Node that provided this topology
    pub nodes: BTreeMap<u32, AvalonEndpoint>,  // All nodes (id -> endpoint)
    pub up: BTreeSet<u32>,             // Set of healthy node IDs
}
```

**Key Functions**:
- `align_offset_to_chunk_boundary(offset)`: Rounds offset down to chunk boundary
- `to_response()` / `from_response()`: Serialize/deserialize for gRPC
- Supports JSON serialization for debugging

### 6. Consistent Hashing (`hash.rs`)

The `AvalonHasher` implements jump consistent hashing for chunk distribution:

```rust
pub struct AvalonHasher {
    num_slots: u16,           // Number of virtual slots (31991, prime)
    servers: Vec<bool>,       // Which server IDs are active
    slot_to_server: Vec<u16>, // Mapping from slot -> server
}
```

**Hash Algorithm**:
1. Uses SipHash-1-3 for fast, uniform hashing
2. Input is the "indicator": `(prefix, path, offset)`
3. Hash is mapped to one of 31,991 virtual slots
4. Slot is then mapped to a server ID

**Slot Assignment**:
- Each server generates a permutation of slots using double hashing
- Slots are assigned round-robin across servers based on permutations
- This ensures balanced distribution and minimal reassignment on topology changes

**Indicator Structure**:
```rust
pub struct Indicator<'a> {
    pub prefix: &'a str,  // Object store prefix (e.g., "s3$mybucket")
    pub path: &'a str,    // Object path within the store
    pub offset: u64,      // Chunk-aligned offset
}
```

## Usage Example

```rust
use lance::dataset::Dataset;

// Configure Avalon endpoints
std::env::set_var("AVALON_ENDPOINTS", "cache1:9090,cache2:9090,cache3:9090");

// Open dataset through Avalon cache
// Inner URL: s3://my-data-bucket/datasets/embeddings
let dataset = Dataset::open("avalon://s3_my-data-bucket/datasets/embeddings").await?;

// All reads now go through Avalon cache
let batch = dataset.take(&[0, 1, 2], ...).await?;
```

## Registration

The Avalon provider is registered in `ObjectStoreRegistry::default()`:

```rust
providers.insert("avalon".into(), Arc::new(AvalonObjectStoreProvider));
```

This makes it available for any Lance operation that uses the `avalon://` scheme.

## Build Configuration

The proto definitions are compiled via `tonic_build` in `build.rs`:

```rust
tonic_build::configure()
    .build_client(true)
    .build_server(true)
    .compile_protos(&["../../protos/avalon.proto"], &["../../protos"])?;
```

## Key Design Decisions

1. **Read-through caching**: Writes bypass the cache entirely, ensuring data consistency
2. **Transparent fallback**: On any Avalon error, reads fall back to the backing store
3. **Chunk-aligned access**: Data is cached in fixed-size chunks for efficient memory management
4. **Consistent hashing**: Ensures stable chunk-to-node mapping and minimal data movement on topology changes
5. **Lazy connection**: Node connections are established only when needed
6. **Bootstrap discovery**: Clients only need to know one endpoint to discover the full cluster

## Future Work (TODOs in proto)

- `Prewarm`: Proactively load data into cache
- `Open/Close`: Session management for temporary handles

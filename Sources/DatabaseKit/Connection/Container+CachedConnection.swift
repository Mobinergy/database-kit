import NIOConcurrencyHelpers

/// Containers that have `DatabasesConfig` structs registered can be used to open, pool, and cache connections.
extension Container {
    // MARK: Cached
    
    /// Returns a Future connection to the `Database` specified by the supplied `DatabaseIdentifier`.
    /// Subsequent calls to this method with the same database ID will return the same connection.
    /// You must call `releaseCachedConnections()` to release the connections.
    ///
    ///     let conn = try app.requestCachedConnection(to: .psql).wait()
    ///     // use conn
    ///     try app.releaseCachedConnections()
    ///
    /// - parameters:
    ///     - database: `DatabaseIdentifier` of a database registered with `Databases`.
    /// - returns: A future containing the `DatabaseConnection`.
    public func requestCachedConnection<Database>(to database: DatabaseIdentifier<Database>) -> Future<Database.Connection> {
        return requestCachedConnection(to: database, poolContainer: self)
    }
    
    /// Returns a Future connection to the `Database` specified by the supplied `DatabaseIdentifier`.
    /// Subsequent calls to this method with the same database ID will return the same connection.
    /// You must call `releaseCachedConnections()` to release the connections.
    ///
    ///     let conn = try app.requestCachedConnection(to: .psql).wait()
    ///     // use conn
    ///     try app.releaseCachedConnections()
    ///
    /// - parameters:
    ///     - database: `DatabaseIdentifier` of a database registered with `Databases`.
    ///     - poolContainer: The container which is used to resolve `DatabaseConnectionPool`.
    /// - returns: A future containing the `DatabaseConnection`.
    public func requestCachedConnection<Database>(to database: DatabaseIdentifier<Database>, poolContainer: Container) -> Future<Database.Connection> {
        do {
            /// use the container to create a connection cache
            /// this must have been registered with the services
            let connections = try make(DatabaseConnectionCache.self)

            /// first get a pointer to the pool
            let pool = try poolContainer.connectionPool(to: database)

            let active = connections.cache.insertValue(forKey: database.uid) {
                /// create an active connection, since we don't have to worry about threading
                /// we can be sure that .connection will be set before this is called again
                let active = CachedDatabaseConnection()
                active.connection = pool.requestConnection().map(to: Database.Connection.self) { conn in
                    /// then create an active connection that knows how to
                    /// release itself
                    active.release = {
                        pool.releaseConnection(conn)
                    }
                    return conn
                }
                return active
            }
            return active.connection as! Future<Database.Connection>
        } catch {
            return eventLoop.newFailedFuture(error: error)
        }
    }

    /// Releases all connections created by calls to `requestCachedConnection(to:)`.
    public func releaseCachedConnections() throws {
        let connections = try make(DatabaseConnectionCache.self)
        return releaseCachedConnections(connections)
    }

    private func releaseCachedConnections(_ connections: DatabaseConnectionCache) {
        let conns = connections.cache.removeAll()
        for (_, conn) in conns {
            guard let release = conn.release else {
                ERROR("Release callback not set.")
                continue
            }
            release()
        }
    }
}

// MARK: Cache Types

/// Caches active database connections. Powers `Container.requestCachedConnection(...)`.
internal final class DatabaseConnectionCache: ServiceType {
    /// Private storage
    fileprivate let cache: ConcurrentDictionary<String, CachedDatabaseConnection>

    /// Creates a new `DatabaseConnectionCache`
    private init() {
        self.cache = .init()
    }

    /// See `ServiceType`.
    static func makeService(for worker: Container) throws -> DatabaseConnectionCache {
        return .init()
    }
}

/// Holds an active connection. Used by `DatabaseConnectionCache`.
private final class CachedDatabaseConnection {
    /// Handles connection release
    typealias OnRelease = () -> ()

    /// The unsafely typed connection
    var connection: Any?

    /// Call this on release
    var release: OnRelease?

    /// Creates a new `ActiveDatabaseConnection`
    init() {}
}

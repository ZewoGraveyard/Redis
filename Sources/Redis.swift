import TCP

public final class Redis: Commands {

	public var conn: TCPConnection

	public init(_ host: String, _ port: Int) throws {
        let uri = URI(host: host, port: port)
        conn = try TCPConnection(to: uri)
	}


}

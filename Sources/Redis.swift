import TCP

public final class Redis: Commands {

	public var conn: TCPConnection

	public init(_ host: String, _ port: Int) throws {
        	conn = try TCPConnection(host: host, port: port)
        	try conn.open()
	}
}

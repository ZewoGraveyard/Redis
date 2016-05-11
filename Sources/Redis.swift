import TCP

public final class Redis: Commands {

	public var conn: TCPConnection
	public var debug: Bool

	public init(_ host: String, _ port: Int, debug: Bool = false) throws {

		self.debug = debug

        conn = try TCPConnection(host: host, port: port)
        try conn.open()
	}
}

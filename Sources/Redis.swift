import TCP

public final class Redis: Commands {

	public var conn: TCPClientSocket

	public init(_ host: String, _ port: Int) throws {
		let ip = try IP(networkInterface: host, port: port)
		conn = try TCPClientSocket(ip: ip)
	}


}
import TCP

public enum CommandError: ErrorType {
	
}

public enum CommandTypeEnum { 
	// String
	case SET(String, String)
	case APPEND(String, String)
	case BITCOUNT(String, Int, Int)
	case BITOP(String, String, Array<String>)
	case BITPOS(String, Int, Array<Int>)
	case INCR(String)
	case INCRBY(String, Int)
	case DECR(String)
	case DECRBY(String, Int)
	case GET(String)
	case GETBIT(String, Int)
	case SETBIT(String, Int, Int)
	case GETRANGE(String, Int, Int)
	case GETSET(String, String)
	case INCRBYFLOAT(String, Float)
	case MGET(Array<String>)
	case MSET(Array<(String, String)>) // this command is an array of (key, value) tuples for a better code reading
	case MSETNX(Array<(String, String)>) // this command is an array of (key, value) tuples for a better code reading
	case SETEX(String, Int, String, Bool) // use the same command for SETEX and PSETEX, but use true in the last parameter for PSETEX
	case SETNX(String, String)
	case SETRANGE(String, Int, String)
	case STRLEN(String)

	// Keys
	case DEL(Array<String>)
	case DUMP(String)
	case EXISTS(Array<String>)
	case EXPIRE(String, Int, Bool) // use the same command for EXPIRE and PEXPIRE, but use true in the last parameter for PEXPIRE
	case EXPIREAT(String, Double, Bool) // use the same command for EXPIREAT and PEXPIREAT, but use true in the last parameter for PEXPIREAT
	case KEYS(String)
	case MOVE(String, Int)
	case PERSIST(String)
	case TTL(String, Bool)
	case RANDOMKEY
	case RENAME(String, String)
	case RENAMENX(String, String)
	case RESTORE(String, Int, String, Bool) // Bool is for REPLACE modifier
	case SORT(String, String) // TODO: implement this madness
	case TYPE(String)

	// Connection
	case AUTH(String)
	case ECHO(String)
	case PING
	case SELECT(Int)

	// For everything else
	case RAW(String)

}

public protocol Commands {
	var conn: TCPClientSocket { get }
}

extension Commands {

	private func send_command(command: String) throws -> Any? {
		try conn.send(command)
		let response = try String(data: try conn.receive(lowWaterMark: 1, highWaterMark: 65536))

		return try Parser.read_response(response)
	}

	public func command(type: CommandTypeEnum) throws -> Any? {

		var result: Any?;

		switch type {
		// MARK: String commands
		case .SET(let key, let value):
			result = try send_command("SET \(key) \"\(value)\"\r\n")

		case .APPEND(let key, let value):
			result = try send_command("APPEND \(key) \"\(value)\"\r\n")

		case .BITCOUNT(let key, let start, let end):
			// TODO: Start and end are optional in Redis. How to do it in Swift?
			result = try send_command("BITCOUNT \(key) \(start) \(end)\r\n")

		case .BITOP(let operation, let destkey, let srckeys):
			result = try send_command("BITOP \(operation) \(destkey) \(srckeys.joinWithSeparator(" "))\r\n")

		case .BITPOS(let key, let bit, let start_end):
			let stringArray = start_end.flatMap { String($0) }
			result = try send_command("BITPOS \(key) \(bit) \(stringArray.joinWithSeparator(" "))\r\n")

		case .INCR(let key):
			result = try send_command("INCR \(key)\r\n")

		case .INCRBY(let key, let increment):
			result = try send_command("INCRBY \(key) \(increment)\r\n")

		case .DECR(let key):
			result = try send_command("DECR \(key)\r\n")

		case .DECRBY(let key, let decrement):
			result = try send_command("DECRBY \(key) \(decrement)\r\n")		

		case .GET(let key):
			result = try send_command("GET \(key)\r\n")

		case .GETBIT(let key, let offset):
			result = try send_command("GETBIT \(key) \(offset)\r\n")

		case .SETBIT(let key, let offset, let value):
			result = try send_command("SETBIT \(key) \(offset) \(value)\r\n")

		case .GETRANGE(let key, let start, let end):
			result = try send_command("GETRANGE \(key) \(start) \(end)\r\n")

		case .GETSET(let key, let value):
			result = try send_command("GETSET \(key) \"\(value)\"\r\n")

		case .INCRBYFLOAT(let key, let increment):
			result = try send_command("INCRBYFLOAT \(key) \(increment)\r\n")

		case .MGET(let keys):
			result = try send_command("MGET \(keys.joinWithSeparator(" "))\r\n")

		case .MSET(let items):
			var cmdStr = ""
			for item in items {
				cmdStr += "\(item.0) \"\(item.1)\" "
			}
			print(cmdStr)

			result = try send_command("MSET \(cmdStr)\r\n")

		// TODO: both cases (MSET, MSETNX) are the same, can it be done in just one with a bool parameter?
		case .MSETNX(let items):
			var cmdStr = ""
			for item in items {
				cmdStr += "\(item.0) \"\(item.1)\" "
			}

			result = try send_command("MSETNX \(cmdStr)\r\n")

		case .SETEX(let key, let expire, let value, let p):
			result = try send_command(p ? "P" : "" + "SETEX \(key) \(expire) \"\(value)\"\r\n")

		case .SETNX(let key, let value):
			result = try send_command("SETNX \(key) \"\(value)\"\r\n")

		case .SETRANGE(let key, let offset, let value):
			result = try send_command("SETRANGE \(key) \(offset) \"\(value)\"\r\n")

		case .STRLEN(let key):
			result = try send_command("STRLEN \(key)\r\n")

		// Keys
		case .DEL(let keys):
			result = try send_command("DEL \(keys.joinWithSeparator(" "))\r\n")

		case .DUMP(let key):
			result = try send_command("DUMP \(key)\r\n")

		case .EXISTS(let keys):
			result = try send_command("EXISTS \(keys.joinWithSeparator(" "))\r\n")

		case .EXPIRE(let key, let seconds, let p):
			result = try send_command(p ? "P" : "" + "EXPIRE \(key) \(seconds)\r\n")

		case .EXPIREAT(let key, let timestamp, let p):
			result = try send_command(p ? "P" : "" + "EXPIREAT \(key) \(timestamp)\r\n")

		case .KEYS(let pattern):
			result = try send_command("KEYS \(pattern)\r\n")

		case .MOVE(let key, let db):
			result = try send_command("MOVE \(key) \(db)\r\n")

		case .PERSIST(let key):
			result = try send_command("PERSIST \(key)\r\n")

		case .TTL(let key, let p):
			result = try send_command(p ? "P" : "" + "TTL \(key)\r\n")

		case .RANDOMKEY:
			result = try send_command("RANDOMKEY\r\n")

		case .RENAME(let key, let newkey):
			result = try send_command("RENAME \(key) \(newkey)\r\n")

		case .RENAMENX(let key, let newkey):
			result = try send_command("RENAMENX \(key) \(newkey)\r\n")

		case .RESTORE(let key, let ttl, let serialized, let replace):
			result = try send_command("RESTORE \(key) \(ttl) \"\(serialized)\"" + (replace ? " REPLACE" : "") + "\r\n")

		case .TYPE(let key):
			result = try send_command("TYPE \(key)\r\n")

		// Connection
		case .AUTH(let password):
			result = try send_command("AUTH \(password)\r\n")
		
		case .ECHO(let message):
			result = try send_command("ECHO \"\(message)\"\r\n")

		case .PING:
			result = try send_command("PING\"\r\n")
		
		case .SELECT(let index):
			result = try send_command("SELECT \(index)\r\n")

		case .RAW(let raw):
			result = try send_command("\(raw)\r\n")

		default:
			result = nil
		}

		return result
	}
	
}









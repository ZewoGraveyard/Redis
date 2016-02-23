import TCP

public enum CommandError: ErrorType {
	
}

public enum CommandTypeEnum { 
	case AUTH 
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
		
		default:
			result = nil
		}

		return result
	}
	
}









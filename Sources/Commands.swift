import TCP

public enum CommandError: ErrorType {
	case WrongNumberOfArguments(numberNeeded: Int)
}

public enum CommandTypeEnum { 
	case AUTH 
	case GET(String)
	case SET(String, String)
	case APPEND(String, String)
	case BITCOUNT(String, Int, Int)
	case BITOP(String, String, Array<String>)
	case BITPOS(String, Int, Array<Int>)
	case INCR(String)
	case INCRBY(String, Int)
	case DECR(String)
	case DECRBY(String, Int)

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
		case .GET(let key):
			result = try send_command("GET \(key)\r\n")

		case .SET(let key, let value):
			result = try send_command("SET \(key) \(value)\r\n")

		case .APPEND(let key, let value):
			result = try send_command("APPEND \(key) \(value)\r\n")

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
			
		default:
			result = ""
		}

		return result
	}
	
}





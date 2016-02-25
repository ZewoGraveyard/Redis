import TCP

public enum CommandError: ErrorType {
	
}

extension Array {
	public func quoteItems() -> Array<String> {
		let result: [String] = self.flatMap({ String("\"\($0)\"") })
		return result
	}
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

	// Lists
	case BLPOP(Array<String>, Int)
	case BRPOP(Array<String>, Int)
	case BRPOPLPUSH(String, String, Int)
	case LINDEX(String, Int)
	case LINSERT(String, String, String, String)
	case LLEN(String)
	case LPOP(String)
	case LPUSH(String, Array<String>)
	case LPUSHX(String, String)
	case LRANGE(String, Int, Int)
	case LREM(String, Int, String)
	case LSET(String, Int, String)
	case LTRIM(String, Int, Int)
	case RPOP(String)
	case RPOPLPUSH(String, String)
	case RPUSH(String, Array<String>)
	case RPUSHX(String, String)

	// Sets
	case SADD(String, Array<String>)
	case SCARD(String)
	case SDIFF(Array<String>)
	case SDIFFSTORE(String, Array<String>)
	case SINTER(Array<String>)
	case SINTERSTORE(String, Array<String>)
	case SISMEMBER(String, String)
	case SMEMBERS(String)
	case SMOVE(String, String, String)
	case SPOP(String)
	case SRANDMEMBER(String, Int?)
	case SREM(String, Array<String>)
	case SUNION(Array<String>)
	case SUNIONSTORE(String, Array<String>)
	

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
			let stringArray = start_end.quoteItems()
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

		case .BLPOP(let keys, let timeout):
			result = try send_command("BLPOP \(keys.joinWithSeparator(" ")) \(timeout))\r\n")

		case .BRPOP(let keys, let timeout):
			result = try send_command("\(keys.joinWithSeparator(" ")) \(timeout)\r\n")

		case .BRPOPLPUSH(let source, let destination, let timeout):
			result = try send_command("BRPOPLPUSH \(source) \"\(destination)\" \(timeout)\r\n")

		case .LINDEX(let key, let index):
			result = try send_command("LINDEX \(key) \(index)\r\n")

		case .LINSERT(let key, let order, let pivot, let value):
			result = try send_command("LINSERT \(key) \(order) \(pivot) \(value)\r\n")

		case .LLEN(let key):
			result = try send_command("LLEN \(key)\r\n")

		case .LPOP(let key):
			result = try send_command("LPOP \(key)\r\n")

		case .LPUSH(let key, let values):
			let newValues = values.quoteItems()
			result = try send_command("LPUSH \(key) \(newValues.joinWithSeparator(" "))\r\n")

		case .LPUSHX(let key, let value):
			result = try send_command("LPUSHX \(key) \"\(value)\"\r\n")

		case .LRANGE(let key, let start, let stop):
			result = try send_command("LRANGE \(key) \(start) \(stop)\r\n")

		case .LREM(let key, let count, let value):
			result = try send_command("LREM \(key) \(count) \"\(value)\"\r\n")

		case .LSET(let key, let index, let value):
			result = try send_command("LSET \(key) \(index) \"\(value)\"\r\n")

		case .LTRIM(let key, let start, let stop):
			result = try send_command("LTRIM \(key) \(start) \(stop)\r\n")

		case .RPOP(let key):
			result = try send_command("RPOP \(key)\r\n")

		case .RPOPLPUSH(let source, let destination):
			result = try send_command("RPOPLPUSH \(source) \(destination)\r\n")

		case .RPUSH(let key, let values):
			let newValues = values.quoteItems()
			result = try send_command("RPUSH \(key) \(newValues.joinWithSeparator(" "))\r\n")

		case .RPUSHX(let key, let value):
			result = try send_command("RPUSHX \(key) \"\(value)\"\r\n")

		// Sets commands
		case .SADD(let key, let members):
			let newValues = members.quoteItems()
			result = try send_command("SADD \(key) \(newValues.joinWithSeparator(" "))\r\n")

		case .SCARD(let key):
			result = try send_command("SCARD \(key)\r\n")

		case .SDIFF(let keys):
			result = try send_command("SDIFF \(keys.joinWithSeparator(" "))\r\n")

		case .SDIFFSTORE(let destination, let keys):
			result = try send_command("SDIFFSTORE \(destination) \(keys.joinWithSeparator(" "))\r\n")

		case .SINTER(let keys):
			result = try send_command("SINTER \(keys.joinWithSeparator(" "))\r\n")

		case .SINTERSTORE(let destination, let keys):
			result = try send_command("SINTERSTORE \(destination) \(keys.joinWithSeparator(" "))\r\n")

		case .SISMEMBER(let key, let member):
			result = try send_command("SISMEMBER \(key) \"\(member)\"\r\n")

		case .SMEMBERS(let key):
			result = try send_command("SMEMBERS \(key)\r\n")

		case .SMOVE(let source, let destination, let member):
			result = try send_command("SMOVE \(source) \(destination) \"\(member)\"\r\n")

		case .SPOP(let key):
			result = try send_command("SPOP \(key)\r\n")

		case .SRANDMEMBER(let key, let count):
			result = try send_command("SRANDMEMBER \(key) \(count != nil ? String(count) : "")\r\n")

		case .SREM(let key, let members):
			let newMembers = members.quoteItems()
			result = try send_command("SREM \(key) \(newMembers.joinWithSeparator(" "))\r\n")

		case .SUNION(let keys):
			result = try send_command("SUNION \(keys.joinWithSeparator(" "))\r\n")

		case .SUNIONSTORE(let destination, let keys):
			result = try send_command("SUNIONSTORE \(destination) \(keys.joinWithSeparator(" "))\r\n")


		case .RAW(let raw):
			result = try send_command("\(raw)\r\n")

		default:
			result = nil
		}

		return result
	}

	public func pipeline(watch: [String] = [], pipe: () throws -> Void) throws -> Any? {

		if watch.count > 0 {
			try send_command("WATCH \(watch.joinWithSeparator(" "))\r\n")
		}

		try send_command("MULTI\r\n")
		try pipe()
		let result = try send_command("EXEC\r\n")

		return result
	}
	
}









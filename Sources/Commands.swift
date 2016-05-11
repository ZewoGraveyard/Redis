import TCP
import String

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

	// Sorted Sets
	case ZADD(String, Dictionary<String, String>) // TODO: add the 3.x arguments
	case ZCARD(String)
	case ZCOUNT(String, String, String)
	case ZINCRBY(String, Float, String)
	case ZINTERSTORE(String, Int, Array<String>, Array<Int>?, String?)
	case ZUNIONSTORE(String, Int, Array<String>, Array<Int>?, String?)
	case ZLEXCOUNT(String, String, String)
	case ZRANGEBYLEX(String, String, String, (Int, Int)?)
	case ZREVRANGEBYLEX(String, String, String, (Int, Int)?)
	case ZRANGE(String, Int, Int, Bool)
	case ZREVRANGE(String, Int, Int, Bool)
	case ZRANGEBYSCORE(String, String, String, Bool, (Int, Int)?)
	case ZREVRANGEBYSCORE(String, String, String, Bool, (Int, Int)?)
	case ZRANK(String, String)
	case ZREVRANK(String, String)
	case ZREM(String, Array<String>)
	case ZREMRANGEBYLEX(String, String, String, (Int, Int)?)
	case ZREMRANGEBYRANK(String, Int, Int)
	case ZREMRANGEBYSCORE(String, String, String)
	case ZSCORE(String, String)


	// Hashes
	case HSET(String, String, String)
	case HSETNX(String, String, String)
	case HDEL(String, Array<String>)
	case HEXISTS(String, String)
	case HGET(String, String)
	case HGETALL(String)
	case HINCRBY(String, String, Int)
	case HINCRBYFLOAT(String, String, Float)
	case HKEYS(String)
	case HLEN(String)
	case HMGET(String, Array<String>)
	case HMSET(String, Dictionary<String, String>)
	case HSTRLEN(String, String)
	case HVALS(String)

	// Connection
	case AUTH(String)
	case ECHO(String)
	case PING
	case SELECT(Int)

	// For everything else
	case RAW(String)

}

public protocol Commands {
	var debug: Bool { get }
	var conn: TCPConnection { get }
}

extension Commands {

	private func send(command: String) throws -> Any? {
		if self.debug == true {
			print("Sending command \(command)")
		}

		try conn.send(command)
        
		let response = try String(data: try conn.receive(upTo: 65536))

		return try Parser.readResponse(response)
	}

	public func command(name: String, values: Any...) throws -> Any? {
		var fullCommand = "\(name)"
		for value in values {

			
			fullCommand = "\(fullCommand) \(value)"
			
		}

		fullCommand = "\(fullCommand)\r\n"

		return try send(command: fullCommand)
	}

	// MARK: String commands
	public func set(key: String, value: String) throws -> String {
		return try command(name: "SET", values: key, "\"\(value)\"") as! String
	}

	public func get(key: String) throws -> String {
		return try command(name: "GET", values: key) as! String
	}

	public func del(keys: [String]) throws -> Int {
		return try command(name: "DEL", values: keys.joined(separator: " ")) as! Int
	}

	public func append(key: String, value: String) throws -> Int {
		return try command(name: "APPEND", values: key, value) as! Int
	}

	public func bitcount(key: String, start: Int? = nil, end: Int? = nil) throws -> Int {
		if start == nil {
			return try command(name: "BITCOUNT", values: key) as! Int
		} else {
			return try command(name: "BITCOUNT", values: key, start!, end!) as! Int
		}
	}

	public func bitop(operation: String, destkey: String, keys: [String]) throws -> Int {
		return try command(name: "BITOP", values: operation, destkey, keys.joined(separator: " ")) as! Int
	}

	public func bitpos(key: String, bit: Int, start: Int? = nil, end: Int? = nil) throws -> Int {
		if start == nil {
			return try command(name: "BITPOS", values: key, bit) as! Int
		} else {
			return try command(name: "BITPOS", values: key, bit, start!, end!) as! Int
		}
	}

	public func incr(key: String) throws -> Int {
		return try command(name: "INCR", values: key) as! Int
	}

	public func incrby(key: String, increment: Int) throws -> Int {
		return try command(name: "INCRBY", values: key, increment) as! Int
	}

	public func decr(key: String) throws -> Int {
		return try command(name: "DECR", values: key) as! Int
	}

	public func decrby(key: String, decrement: Int) throws -> Int {
		return try command(name: "DECRBY", values: key, decrement) as! Int
	}

	public func getbit(key: String, offset: Int) throws -> Int {
		return try command(name: "GETBIT", values: key, offset) as! Int
	}

	public func setbit(key: String, offset: Int, value: Int) throws -> Int {
		return try command(name: "SETBIT", values: key, offset, value) as! Int
	}

	public func getrange(key: String, start: Int, end: Int) throws -> String {
		return try command(name: "GETRANGE", values: key, start, end) as! String
	}

	public func getset(key: String, value: String) throws -> String {
		return try command(name: "GETSET", values: key, value) as! String
	}

	public func incrbyfloat(key: String, increment: Float) throws -> Float {
		var result = try command(name: "INCRBYFLOAT", values: key, increment) as! String
		result.replace(string: "\r\n", with: "")
		return Float(result.trim())!
	}

	public func mget(keys: [String]) throws -> String {
		return try command(name: "MGET", values: keys.joined(separator: " ")) as! String
	}

	public func mset(items: [(String, String)]) throws -> String {
		let cmd = items.map { return "\($0) \($1)" }.reduce("", combine: { (first: String, second: String) -> String in  
			return "\(first) \(second)"  
		})

		return try command(name: "MSET", values: cmd) as! String
	}

	public func msetnx(items: [(String, String)]) throws -> Int {
		let cmd = items.map { return "\($0) \($1)" }.reduce("", combine: { (first: String, second: String) -> String in  
			return "\(first) \(second)"  
		})

		return try command(name: "MSETNX", values: cmd) as! Int
	}
	
	public func setex(key: String, seconds: Int, value: String) throws -> String {
		return try command(name: "SETEX", values: key, seconds, "\"\(value)\"") as! String
	}

	public func setnx(key: String, value: String) throws -> Int {
		return try command(name: "SETNX", values: key, "\"\(value)\"") as! Int
	}

	public func setrange(key: String, offset: Int, value: String) throws -> Int {
		return try command(name: "SETRANGE", values: key, offset, "\"\(value)\"") as! Int
	}

	public func strlen(key: String) throws -> Int {
		return try command(name: "STRLEN", values: key) as! Int
	}

	// Keys commands
	// TODO: there is a bug when receiving the response of this command
	public func dump(key: String) throws -> String {
		return try command(name: "DUMP", values: key) as! String
	}

	public func exists(key: String) throws -> Int {
		return try command(name: "EXISTS", values: key) as! Int
	}

	public func expire(key: String, seconds: Int) throws -> Int {
		return try command(name: "EXPIRE", values: key, seconds) as! Int
	}

	public func expireat(key: String, timestamp: Int) throws -> Int {
		return try command(name: "EXPIREAT", values: key, timestamp) as! Int
	}

	public func keys(pattern: String) throws -> [Any?] {
		return try command(name: "KEYS", values: pattern) as! [Any?]
	}

	public func move(key: String, db: Int) throws -> Int {
		return try command(name: "MOVE", values: key, db) as! Int
	}

	public func persist(key: String) throws -> Int {
		return try command(name: "PERSIST", values: key) as! Int
	}

	public func ttl(key: String) throws -> Int {
		return try command(name: "TTL", values: key) as! Int
	}

	public func pttl(key: String) throws -> Int {
		return try command(name: "PTTL", values: key) as! Int
	}

	public func randomkey() throws -> String {
		return try command(name: "RANDOMKEY") as! String
	}

	public func rename(key: String, newkey: String) throws -> String {
		return try command(name: "RENAME", values: key, newkey) as! String
	}

	public func renamenx(key: String, newkey: String) throws -> Int {
		return try command(name: "RENAMENX", values: key, newkey) as! Int
	}

	public func restore(key: String, ttl: Int, serialized: String, replace: Bool = false) throws -> String {
		return try command(name: "RESTORE", values: key, ttl, "\"\(serialized)\"", (replace == true ? "REPLACE" : "")) as! String
	}

	public func type(key: String) throws -> String {
		return try command(name: "TYPE", values: key) as! String
	}

	// Connection commands
	public func auth(password: String) throws -> String {
		return try command(name: "AUTH", values: password) as! String
	}

	public func echo(message: String) throws -> String {
		return try command(name: "ECHO", values: message) as! String
	}

	public func ping() throws -> String {
		return try command(name: "PING") as! String
	}

	public func select(index: String) throws -> String {
		return try command(name: "SELECT", values: index) as! String
	}

	// List commands
	public func blpop(keys: [String], timeout: Int) throws -> [Any?] {
		return try command(name: "BLPOP", values: keys.joined(separator: " "), timeout) as! [Any?]
	}
	
	public func brpop(keys: [String], timeout: Int) throws -> [Any?] {
		return try command(name: "BRPOP", values: keys.joined(separator: " "), timeout) as! [Any?]
	}

	public func brpoplpush(source: String, destination: String, timeout: Int) throws -> String {
		return try command(name: "BRPOPLPUSH", values: source, destination, timeout) as! String
	}

	public func lindex(key: String, index: Int) throws -> String {
		return try command(name: "LINDEX", values: key, index) as! String
	}

	public func linsert(key: String, order: String, pivot: String, value: String) throws -> Int {
		return try command(name: "LINSERT", values: key, order, "\"\(pivot)\"", "\"\(value)\"") as! Int
	}

	public func llen(key: String) throws -> Int {
		return try command(name: "LLEN", values: key) as! Int
	}

	public func lpop(key: String) throws -> String {
		return try command(name: "LPOP", values: key) as! String
	}

	public func lpush(key: String, values: [String]) throws -> Int {
		let newValues = values.quoteItems()
		return try command(name: "LPUSH", values: key, newValues.joined(separator: " ")) as! Int
	}

	public func lpushx(key: String, value: String) throws -> Int {
		return try command(name: "LPUSHX", values: key, "\"\(value)\"") as! Int
	}

	public func lrange(key: String, start: Int, stop: Int) throws -> [Any?] {
		return try command(name: "LRANGE", values: key, start, stop) as! [Any?]
	}

	public func lrem(key: String, count: Int, value: String) throws -> Int {
		return try command(name: "LREM", values: key, count, "\"\(value)\"") as! Int
	}

	public func lset(key: String, index: Int, value: String) throws -> String {
		return try command(name: "LSET", values: key, index, "\"\(value)\"") as! String
	}

	public func ltrim(key: String, start: Int, stop: Int) throws -> String {
		return try command(name: "LTRIM", values: key, start, stop) as! String
	}

	public func rpop(key: String) throws -> String {
		return try command(name: "RPOP", values: key) as! String
	}

	public func rpoplpush(source: String, destination: String) throws -> String {
		return try command(name: "RPOPLPUSH", values: source, destination) as! String
	}

	public func rpush(key: String, values: [String]) throws -> Int {
		let newValues = values.quoteItems()
		return try command(name: "RPUSH", values: key, newValues.joined(separator: " ")) as! Int
	}

	public func rpushx(key: String, value: String) throws -> Int {
		return try command(name: "RPUSHX", values: key, "\"\(value)\"") as! Int
	}

	public func command(type: CommandTypeEnum) throws -> Any? {

		var result: Any?;

		switch type {

		// Sets commands
		case .SADD(let key, let members):
			let newValues = members.quoteItems()
			result = try send(command:"SADD \(key) \(newValues.joined(separator: " "))\r\n")

		case .SCARD(let key):
			result = try send(command:"SCARD \(key)\r\n")

		case .SDIFF(let keys):
			result = try send(command:"SDIFF \(keys.joined(separator: " "))\r\n")

		case .SDIFFSTORE(let destination, let keys):
			result = try send(command:"SDIFFSTORE \(destination) \(keys.joined(separator: " "))\r\n")

		case .SINTER(let keys):
			result = try send(command:"SINTER \(keys.joined(separator: " "))\r\n")

		case .SINTERSTORE(let destination, let keys):
			result = try send(command:"SINTERSTORE \(destination) \(keys.joined(separator: " "))\r\n")

		case .SISMEMBER(let key, let member):
			result = try send(command:"SISMEMBER \(key) \"\(member)\"\r\n")

		case .SMEMBERS(let key):
			result = try send(command:"SMEMBERS \(key)\r\n")

		case .SMOVE(let source, let destination, let member):
			result = try send(command:"SMOVE \(source) \(destination) \"\(member)\"\r\n")

		case .SPOP(let key):
			result = try send(command:"SPOP \(key)\r\n")

		case .SRANDMEMBER(let key, let count):
			result = try send(command:"SRANDMEMBER \(key) \(count != nil ? String(count) : "")\r\n")

		case .SREM(let key, let members):
			let newMembers = members.quoteItems()
			result = try send(command:"SREM \(key) \(newMembers.joined(separator: " "))\r\n")

		case .SUNION(let keys):
			result = try send(command:"SUNION \(keys.joined(separator: " "))\r\n")

		case .SUNIONSTORE(let destination, let keys):
			result = try send(command:"SUNIONSTORE \(destination) \(keys.joined(separator: " "))\r\n")

		// Sorted Sets
		case .ZADD(let key, let values):
			let strValues = values.reduce(String()) { str, pair in
				var tmp = ""
				if str != "" {
					tmp = "\(str) "
				}
				tmp += "\(pair.0) \"\(pair.1)\""
				return tmp
			}

			result = try send(command:"ZADD \(key) \(strValues)\r\n")

		case .ZCARD(let key):
			result = try send(command:"ZCARD \(key)\r\n")

		case .ZCOUNT(let key, let min, let max):
			result = try send(command:"ZCOUNT \(key) \(min) \(max)\r\n")

		case .ZINCRBY(let key, let increment, let member):
			result = try send(command:"ZINCRBY \(key) \(increment) \"\(member)\"\r\n")

		case .ZINTERSTORE(let destination, let numkeys, let keys, let weights, let aggregate):
			var cmd = "\(destination) \(numkeys) \(keys.joined(separator: " "))"

			if weights != nil {
				cmd = "\(cmd) WEIGHTS \(weights)"
			}

			if aggregate != nil {
				cmd = "\(cmd) AGGREGATE \(aggregate)"
			}

			result = try send(command:"ZINTERSTORE \(cmd)\r\n")

		case .ZUNIONSTORE(let destination, let numkeys, let keys, let weights, let aggregate):
			var cmd = "\(destination) \(numkeys) \(keys.joined(separator: " "))"

			if weights != nil {
				cmd = "\(cmd) WEIGHTS \(weights)"
			}

			if aggregate != nil {
				cmd = "\(cmd) AGGREGATE \(aggregate)"
			}

			result = try send(command:"ZUNIONSTORE \(cmd)\r\n")

		case .ZLEXCOUNT(let key, let min, let max):
			result = try send(command:"ZLEXCOUNT \(key) \(min) \(max)\r\n")

		case .ZRANGEBYLEX(let key, let min, let max, let limit):
			var cmd = "\(key) \(min) \(max)"

			if limit != nil {
				cmd = "\(cmd) LIMIT \(limit!.0) \(limit!.1)"
			}

			result = try send(command:"ZRANGEBYLEX \(cmd)\r\n")

		case .ZREVRANGEBYLEX(let key, let max, let min, let limit):
			var cmd = "\(key) \(max) \(min)"

			if limit != nil {
				cmd = "\(cmd) LIMIT \(limit!.0) \(limit!.1)"
			}

			result = try send(command:"ZRANGEBYLEX \(cmd)\r\n")

		case .ZRANGE(let key, let start, let stop, let withscores):
			var cmd = "\(key) \(start) \(stop)"

			if withscores {
				cmd = "\(cmd) WITHSCORES"
			}

			result = try send(command:"ZRANGE \(cmd)\r\n")

		case .ZREVRANGE(let key, let start, let stop, let withscores):
			var cmd = "\(key) \(start) \(stop)"

			if withscores {
				cmd = "\(cmd) WITHSCORES"
			}

			result = try send(command:"ZREVRANGE \(cmd)\r\n")

		case .ZRANGEBYSCORE(let key, let min, let max, let withscores, let limit):
			var cmd = "\(key) \(min) \(max)"

			if withscores {
				cmd = "\(cmd) WITHSCORES"
			}

			if limit != nil {
				cmd = "\(cmd) LIMIT \(limit!.0) \(limit!.1)"
			}

			result = try send(command:"ZRANGEBYSCORE \(cmd)\r\n")

		case .ZREVRANGEBYSCORE(let key, let max, let min, let withscores, let limit):
			var cmd = "\(key) \(max) \(min)"

			if withscores {
				cmd = "\(cmd) WITHSCORES"
			}

			if limit != nil {
				cmd = "\(cmd) LIMIT \(limit!.0) \(limit!.1)"
			}

			result = try send(command:"ZRANGEBYSCORE \(cmd)\r\n")

		case .ZRANK(let key, let member):
			result = try send(command:"ZRANK \(key) \"\(member)\"\r\n")

		case .ZREVRANK(let key, let member):
			result = try send(command:"ZREVRANK \(key) \"\(member)\"\r\n")

		case .ZREM(let key, let members):
			result = try send(command:"ZREM \(key) \"\(members.joined(separator: "\" \""))\"\r\n")

		case .ZREMRANGEBYLEX(let key, let min, let max, let limit):
			var cmd = "\(key) \(min) \(max)"

			if limit != nil {
				cmd = "\(cmd) LIMIT \(limit!.0) \(limit!.1)"
			}

			result = try send(command:"ZREMRANGEBYLEX \(cmd)\r\n")

		case .ZREMRANGEBYRANK(let key, let start, let stop):
			result = try send(command:"ZREMRANGEBYRANK \(key) \(start) \(stop)\r\n")

		case .ZREMRANGEBYSCORE(let key, let min, let max):
			result = try send(command:"ZREMRANGEBYSCORE \(key) \(min) \(max)\r\n")

		case .ZSCORE(let key, let member):
			result = try send(command:"ZSCORE \(key) \"\(member)\"\r\n")

		// Hashes
		case .HSET(let key, let field, let value):
			result = try send(command:"HSET \(key) \(field) \"\(value)\"\r\n")

		case .HSETNX(let key, let field, let value):
			result = try send(command:"HSETNX \(key) \(field) \"\(value)\"\r\n")

		case .HDEL(let key, let fields):
			result = try send(command:"HDEL \(key) \(fields.joined(separator: " "))\r\n")

		case .HEXISTS(let key, let field):
			result = try send(command:"HEXISTS \(key) \(field)\r\n")

		case .HGET(let key, let field):
			result = try send(command:"HGET \(key) \(field)\r\n")

		case .HGETALL(let key):
			result = try send(command:"HGETALL \(key)\r\n")

		case .HINCRBY(let key, let field, let increment):
			result = try send(command:"HINCRBY \(key) \(field) \(increment)\r\n")

		case .HINCRBYFLOAT(let key, let field, let increment):
			result = try send(command:"HINCRBYFLOAT \(key) \(field) \(increment)\r\n")

		case .HKEYS(let key):
			result = try send(command:"HKEYS \(key)\r\n")

		case .HLEN(let key):
			result = try send(command:"HLEN \(key)\r\n")

		case .HMGET(let key, let fields):
			result = try send(command:"HMGET \(key) \(fields.joined(separator: " "))\r\n")

		case .HMSET(let key, let values):
			let strValues = values.reduce(String()) { str, pair in
				var tmp = ""
				if str != "" {
					tmp = "\(str) "
				}
				tmp += "\(pair.0) \"\(pair.1)\""
				return tmp
			}

			result = try send(command:"HMSET \(key) \(strValues)\r\n")

		case .HSTRLEN(let key, let field):
			result = try send(command:"HSTRLEN \(key) \(field)\r\n")

		case .HVALS(let key):
			result = try send(command:"HVALS \(key)\r\n")


		case .RAW(let raw):
			result = try send(command:"\(raw)\r\n")

		default:
			result = nil
		}

		return result
	}

    public func pipeline(watch: [String] = [], discards: Bool, pipe: () throws -> Void) throws -> Any? {
		if watch.count > 0 {
			try send(command:"WATCH \(watch.joined(separator: " "))\r\n")
		}

		try send(command:"MULTI\r\n")
        
        if discards {
            do {
                try pipe()
            }
            catch {
                return try send(command:"DISCARD\r\n")
            }
        }
        else {
            try pipe()
        }
		
		return try send(command:"EXEC\r\n")
	}
	
}


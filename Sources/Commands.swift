import TCP

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
	var conn: TCPConnection { get }
}

extension Commands {

	private func send(command: String) throws -> Any? {
		try conn.send(command)
        
		let response = try String(data: try conn.receive(upTo: 65536))

    return try Parser.read_response(fullResponse: response)
	}

	public func command(type: CommandTypeEnum) throws -> Any? {

		var result: Any?;

		switch type {
		// MARK: String commands
		case .SET(let key, let value):
			result = try send(command:"SET \(key) \"\(value)\"\r\n")

		case .APPEND(let key, let value):
			result = try send(command:"APPEND \(key) \"\(value)\"\r\n")

		case .BITCOUNT(let key, let start, let end):
			// TODO: Start and end are optional in Redis. How to do it in Swift?
			result = try send(command:"BITCOUNT \(key) \(start) \(end)\r\n")

		case .BITOP(let operation, let destkey, let srckeys):
			result = try send(command:"BITOP \(operation) \(destkey) \(srckeys.joined(separator: " "))\r\n")

		case .BITPOS(let key, let bit, let start_end):
			let stringArray = start_end.quoteItems()
			result = try send(command:"BITPOS \(key) \(bit) \(stringArray.joined(separator: " "))\r\n")

		case .INCR(let key):
			result = try send(command:"INCR \(key)\r\n")

		case .INCRBY(let key, let increment):
			result = try send(command:"INCRBY \(key) \(increment)\r\n")

		case .DECR(let key):
			result = try send(command:"DECR \(key)\r\n")

		case .DECRBY(let key, let decrement):
			result = try send(command:"DECRBY \(key) \(decrement)\r\n")		

		case .GET(let key):
			result = try send(command:"GET \(key)\r\n")

		case .GETBIT(let key, let offset):
			result = try send(command:"GETBIT \(key) \(offset)\r\n")

		case .SETBIT(let key, let offset, let value):
			result = try send(command:"SETBIT \(key) \(offset) \(value)\r\n")

		case .GETRANGE(let key, let start, let end):
			result = try send(command:"GETRANGE \(key) \(start) \(end)\r\n")

		case .GETSET(let key, let value):
			result = try send(command:"GETSET \(key) \"\(value)\"\r\n")

		case .INCRBYFLOAT(let key, let increment):
			result = try send(command:"INCRBYFLOAT \(key) \(increment)\r\n")

		case .MGET(let keys):
			result = try send(command:"MGET \(keys.joined(separator: " "))\r\n")

		case .MSET(let items):
			var cmdStr = ""
			for item in items {
				cmdStr += "\(item.0) \"\(item.1)\" "
			}
			print(cmdStr)

			result = try send(command:"MSET \(cmdStr)\r\n")

		// TODO: both cases (MSET, MSETNX) are the same, can it be done in just one with a bool parameter?
		case .MSETNX(let items):
			var cmdStr = ""
			for item in items {
				cmdStr += "\(item.0) \"\(item.1)\" "
			}

			result = try send(command:"MSETNX \(cmdStr)\r\n")

		case .SETEX(let key, let expire, let value, let p):
			result = try send(command:p ? "P" : "" + "SETEX \(key) \(expire) \"\(value)\"\r\n")

		case .SETNX(let key, let value):
			result = try send(command:"SETNX \(key) \"\(value)\"\r\n")

		case .SETRANGE(let key, let offset, let value):
			result = try send(command:"SETRANGE \(key) \(offset) \"\(value)\"\r\n")

		case .STRLEN(let key):
			result = try send(command:"STRLEN \(key)\r\n")

		// Keys
		case .DEL(let keys):
			result = try send(command:"DEL \(keys.joined(separator: " "))\r\n")

		case .DUMP(let key):
			result = try send(command:"DUMP \(key)\r\n")

		case .EXISTS(let keys):
			result = try send(command:"EXISTS \(keys.joined(separator: " "))\r\n")

		case .EXPIRE(let key, let seconds, let p):
			result = try send(command:p ? "P" : "" + "EXPIRE \(key) \(seconds)\r\n")

		case .EXPIREAT(let key, let timestamp, let p):
			result = try send(command:p ? "P" : "" + "EXPIREAT \(key) \(timestamp)\r\n")

		case .KEYS(let pattern):
			result = try send(command:"KEYS \(pattern)\r\n")

		case .MOVE(let key, let db):
			result = try send(command:"MOVE \(key) \(db)\r\n")

		case .PERSIST(let key):
			result = try send(command:"PERSIST \(key)\r\n")

		case .TTL(let key, let p):
			result = try send(command:p ? "P" : "" + "TTL \(key)\r\n")

		case .RANDOMKEY:
			result = try send(command:"RANDOMKEY\r\n")

		case .RENAME(let key, let newkey):
			result = try send(command:"RENAME \(key) \(newkey)\r\n")

		case .RENAMENX(let key, let newkey):
			result = try send(command:"RENAMENX \(key) \(newkey)\r\n")

		case .RESTORE(let key, let ttl, let serialized, let replace):
			result = try send(command:"RESTORE \(key) \(ttl) \"\(serialized)\"" + (replace ? " REPLACE" : "") + "\r\n")

		case .TYPE(let key):
			result = try send(command:"TYPE \(key)\r\n")

		// Connection
		case .AUTH(let password):
			result = try send(command:"AUTH \(password)\r\n")
		
		case .ECHO(let message):
			result = try send(command:"ECHO \"\(message)\"\r\n")

		case .PING:
			result = try send(command:"PING\"\r\n")
		
		case .SELECT(let index):
			result = try send(command:"SELECT \(index)\r\n")

		case .BLPOP(let keys, let timeout):
			result = try send(command:"BLPOP \(keys.joined(separator: " ")) \(timeout))\r\n")

		case .BRPOP(let keys, let timeout):
			result = try send(command:"\(keys.joined(separator: " ")) \(timeout)\r\n")

		case .BRPOPLPUSH(let source, let destination, let timeout):
			result = try send(command:"BRPOPLPUSH \(source) \"\(destination)\" \(timeout)\r\n")

		case .LINDEX(let key, let index):
			result = try send(command:"LINDEX \(key) \(index)\r\n")

		case .LINSERT(let key, let order, let pivot, let value):
			result = try send(command:"LINSERT \(key) \(order) \(pivot) \(value)\r\n")

		case .LLEN(let key):
			result = try send(command:"LLEN \(key)\r\n")

		case .LPOP(let key):
			result = try send(command:"LPOP \(key)\r\n")

		case .LPUSH(let key, let values):
			let newValues = values.quoteItems()
			result = try send(command:"LPUSH \(key) \(newValues.joined(separator: " "))\r\n")

		case .LPUSHX(let key, let value):
			result = try send(command:"LPUSHX \(key) \"\(value)\"\r\n")

		case .LRANGE(let key, let start, let stop):
			result = try send(command:"LRANGE \(key) \(start) \(stop)\r\n")

		case .LREM(let key, let count, let value):
			result = try send(command:"LREM \(key) \(count) \"\(value)\"\r\n")

		case .LSET(let key, let index, let value):
			result = try send(command:"LSET \(key) \(index) \"\(value)\"\r\n")

		case .LTRIM(let key, let start, let stop):
			result = try send(command:"LTRIM \(key) \(start) \(stop)\r\n")

		case .RPOP(let key):
			result = try send(command:"RPOP \(key)\r\n")

		case .RPOPLPUSH(let source, let destination):
			result = try send(command:"RPOPLPUSH \(source) \(destination)\r\n")

		case .RPUSH(let key, let values):
			let newValues = values.quoteItems()
			result = try send(command:"RPUSH \(key) \(newValues.joined(separator: " "))\r\n")

		case .RPUSHX(let key, let value):
			result = try send(command:"RPUSHX \(key) \"\(value)\"\r\n")

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


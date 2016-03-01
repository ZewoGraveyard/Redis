import Venice

public class PubSub {

	var connection: Redis

	private var run: Bool

	public init(conn: Redis) {
		self.connection = conn
		self.run = false
	}

	public func subscribe(channels: [String], callback: (message: [String: Any?]) -> Void) throws {
		let initial: [Any?] = try self.connection.command(.RAW("SUBSCRIBE \(channels.joinWithSeparator(" "))")) as! Array
		
		for i in 0..<initial.count {
			// multiple channels means multiple callbacks
			let current: [Any?] = initial[i] as! Array
			callback(message: ["type": current[0], 
								"channel": current[1],
								"data": current[2]])
		}
		
		self.run = true
		while self.run {
			do {
				let response = try String(data: try self.connection.conn.receive(lowWaterMark: 1, highWaterMark: 65536))
				let parsed: [Any?] = try Parser.read_response(response) as! Array
				callback(message: ["type": parsed[0], 
									"channel": parsed[1],
									"data": parsed[2]])
			} catch {
				// TODO: should throw something?
			}
		}
	}

	public func unsubscribe(channel: String) -> [String: Any?]? {

		do {
			let unsub: [Any?] = try self.connection.command(.RAW("UNSUBSCRIBE \(channel)")) as! Array

			if unsub[2] as? Int == 0 {
				self.run = false
			}

			return ["type": unsub[0], 
					"channel": unsub[1],
					"data": unsub[2]]
		} catch {
			// TODO: should throw something?
			return nil
		}
	}

}





extension String {

    public func indexOf(character char: Character) -> Int? {
        if let idx = self.characters.index(of: char) {
            return self.distance(from: self.startIndex, to: idx)
        }
        return nil
    }
}

public enum ResponseError: ErrorProtocol {
	case invalidResponse(byte: Character, response: String)
	case serverError(error: String)

}

// For more info: http://redis.io/topics/protocol
struct Parser {

	static func readResponse(_ fullResponse: String) throws -> Any? {

		let byte: Character = fullResponse[fullResponse.startIndex]
    let response: String = fullResponse[fullResponse.index(fullResponse.startIndex, offsetBy: 1)..<fullResponse.endIndex]

		var result: Any?

		guard ["+", "-", ":", "$", "*"].index(of: byte) != nil else { throw ResponseError.invalidResponse(byte: byte, response: response) }

		switch byte {
		case "-":
			// Server error
			throw ResponseError.serverError(error: response)
		case ":":
			// Simple integer
			let idx = Int(response.indexOf(character: "\r\n")!)
			result = Int(response[response.startIndex..<response.index(response.startIndex, offsetBy: idx)])
		case "+":
			// Simple scleng
			result = String(response)
		case "$":
			// Bulk string
			let idx = Int(response.indexOf(character: "\r\n")!)
			if response[response.startIndex..<response.index(response.startIndex, offsetBy: idx)] == "-1" {
				// nil string
				result = nil
			} else {
				result = String(response[response.index(response.startIndex, offsetBy: idx)..<response.endIndex])
			}

		case "*":
			// Arrays
			var values = response.characters.split(separator: "\r\n").map(String.init)
			var parsed: [Any?] = []
			
			// check if Redis returned a null array
			if values[0] == "-1" {
				result = nil
			} else {
				values[0] = "*\(values[0])"

				while values.count > 0 {
					var tmp: [Any?] = []
          let tail: Int = Int(String(values[0][values[0].index(values[0].startIndex, offsetBy: 1)]))!

					values.remove(at: 0)
					while tmp.count < tail {

						if values[0][values[0].startIndex] != "$" {
							tmp.append(try Parser.readResponse("\(values[0])\r\n"))
						} else {
							values.remove(at: 0)
							tmp.append(values[0])
						}

						values.remove(at: 0)
					}

					parsed.append(tmp)
				}

				if parsed.count == 1 {
					// return as simple array
					result = parsed[0]
				} else {
					result = parsed
				}
			}

		default:
			result = ""
		}

		return result

	}

}
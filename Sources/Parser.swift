
extension String {

    public func indexOfCharacter(char: Character) -> Int? {
        if let idx = self.characters.index(of: char) {
            return self.startIndex.distance(to: idx)
        }
        return nil
    }
}

public enum ResponseError: ErrorProtocol {
	case InvalidResponse(byte: Character, response: String)
	case ServerError(error: String)

}

// For more info: http://redis.io/topics/protocol
struct Parser {

	static func read_response(fullResponse: String) throws -> Any? {

		let byte: Character = fullResponse[fullResponse.startIndex]
		let response: String = fullResponse[fullResponse.startIndex.advanced(by: 1)..<fullResponse.endIndex]

		var result: Any?

		guard ["+", "-", ":", "$", "*"].index(of: byte) != nil else { throw ResponseError.InvalidResponse(byte: byte, response: response) }

		switch byte {
		case "-":
			// Server error
			throw ResponseError.ServerError(error: response)
		case ":":
			// Simple integer
      let idx = Int(response.indexOfCharacter(char: "\r\n")!)
			result = Int(response[response.startIndex..<response.startIndex.advanced(by: idx)])
		case "+":
			// Simple scleng
			result = String(response)
		case "$":
			// Bulk string
      let idx = Int(response.indexOfCharacter(char: "\r\n")!)
			if response[response.startIndex..<response.startIndex.advanced(by: idx)] == "-1" {
				// nil string
				result = nil
			} else {
				result = String(response[response.startIndex.advanced(by: idx)..<response.endIndex])
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
					let tail: Int = Int(String(values[0][values[0].startIndex.advanced(by: 1)]))!

					values.remove(at: 0)
					while tmp.count < tail {

						if values[0][values[0].startIndex] != "$" {
              tmp.append(try Parser.read_response(fullResponse: "\(values[0])\r\n"))
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
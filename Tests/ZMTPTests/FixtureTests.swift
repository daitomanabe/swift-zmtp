import Foundation
import Testing
import ZMTP

/// Byte streams between two libzmq peers, recorded by Tools/interop/record_fixtures.py.
struct Fixture: Decodable, CustomTestStringConvertible {
    let name: String
    let libzmq: String
    let clientToServer: String
    let serverToClient: String

    var testDescription: String { name }

    static func all() throws -> [Fixture] {
        guard let directory = Bundle.module.url(forResource: "Fixtures", withExtension: nil) else {
            throw CocoaError(.fileNoSuchFile)
        }
        let files = try FileManager.default.contentsOfDirectory(at: directory, includingPropertiesForKeys: nil)
            .filter { $0.pathExtension == "json" }
            .sorted { $0.lastPathComponent < $1.lastPathComponent }
        return try files.map { try JSONDecoder().decode(Fixture.self, from: Data(contentsOf: $0)) }
    }
}

func bytes(hex: String) -> [UInt8] {
    var result: [UInt8] = []
    result.reserveCapacity(hex.utf8.count / 2)
    var index = hex.startIndex
    while index < hex.endIndex {
        let next = hex.index(index, offsetBy: 2)
        result.append(UInt8(hex[index..<next], radix: 16)!)
        index = next
    }
    return result
}

@Test(arguments: try Fixture.all())
func streamsOpenWithZMTP3Greeting(_ fixture: Fixture) {
    for stream in [fixture.clientToServer, fixture.serverToClient].map(bytes(hex:)) {
        #expect(stream.count >= ZMTP.greetingLength)
        #expect(stream.first == 0xFF)
        #expect(stream[ZMTP.signatureLength - 1] == 0x7F)
        #expect(stream[ZMTP.signatureLength] == ZMTP.majorVersion)
    }
}

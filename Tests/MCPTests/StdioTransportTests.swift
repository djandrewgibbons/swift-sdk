import Foundation
import Testing

@testable import MCP

#if canImport(System)
    import System
#else
    @preconcurrency import SystemPackage
#endif

@Suite("Stdio Transport Tests")
struct StdioTransportTests {
    @Test("Connection")
    func testStdioTransportConnection() async throws {
        let (input, _) = try FileDescriptor.pipe()
        let (_, output) = try FileDescriptor.pipe()
        let transport = StdioTransport(input: input, output: output, logger: nil)
        try await transport.connect()
        await transport.disconnect()
    }

    @Test("Send Message")
    func testStdioTransportSendMessage() async throws {
        let (reader, output) = try FileDescriptor.pipe()
        let (input, _) = try FileDescriptor.pipe()
        let transport = StdioTransport(input: input, output: output, logger: nil)
        try await transport.connect()

        // Test sending a simple message
        let message = #"{"key":"value"}"#
        try await transport.send(message.data(using: .utf8)!)

        // Read and verify the output
        var buffer = [UInt8](repeating: 0, count: 1024)
        let bytesRead = try buffer.withUnsafeMutableBufferPointer { pointer in
            try reader.read(into: UnsafeMutableRawBufferPointer(pointer))
        }
        let data = Data(buffer[..<bytesRead])
        let expectedOutput = message.data(using: .utf8)! + "\n".data(using: .utf8)!
        #expect(data == expectedOutput)

        await transport.disconnect()
    }

    @Test("Receive Message")
    func testStdioTransportReceiveMessage() async throws {
        let (input, writer) = try FileDescriptor.pipe()
        let (_, output) = try FileDescriptor.pipe()
        let transport = StdioTransport(input: input, output: output, logger: nil)
        try await transport.connect()

        // Write test message to input pipe
        let message = ["key": "value"]
        let messageData = try JSONEncoder().encode(message) + "\n".data(using: .utf8)!
        try writer.writeAll(messageData)
        try writer.close()

        // Start receiving messages
        let stream: AsyncThrowingStream<Data, Swift.Error> = await transport.receive()
        var iterator = stream.makeAsyncIterator()

        // Get first message
        let received = try await iterator.next()
        #expect(received == #"{"key":"value"}"#.data(using: .utf8)!)

        await transport.disconnect()
    }

    @Test("Invalid JSON")
    func testStdioTransportInvalidJSON() async throws {
        let (input, writer) = try FileDescriptor.pipe()
        let (_, output) = try FileDescriptor.pipe()
        let transport = StdioTransport(input: input, output: output, logger: nil)
        try await transport.connect()

        // Write invalid JSON to input pipe
        let invalidJSON = #"{ invalid json }"#
        try writer.writeAll(invalidJSON.data(using: .utf8)!)
        try writer.close()

        let stream: AsyncThrowingStream<Data, Swift.Error> = await transport.receive()
        var iterator = stream.makeAsyncIterator()

        _ = try await iterator.next()

        await transport.disconnect()
    }

    @Test("Send Error")
    func testStdioTransportSendError() async throws {
        let (input, _) = try FileDescriptor.pipe()
        let transport = StdioTransport(
            input: input,
            output: FileDescriptor(rawValue: -1),  // Invalid fd
            logger: nil
        )

        do {
            try await transport.connect()
            #expect(Bool(false), "Expected connect to throw an error")
        } catch {
            #expect(error is MCPError)
        }

        await transport.disconnect()
    }

    @Test("Receive Error")
    func testStdioTransportReceiveError() async throws {
        let (_, output) = try FileDescriptor.pipe()
        let transport = StdioTransport(
            input: FileDescriptor(rawValue: -1),  // Invalid fd
            output: output,
            logger: nil
        )

        do {
            try await transport.connect()
            #expect(Bool(false), "Expected connect to throw an error")
        } catch {
            #expect(error is MCPError)
        }

        await transport.disconnect()
    }

    @Test("Receive Messages Split Across Chunks", .timeLimit(.minutes(1)))
    func testStdioTransportReceiveChunkedMessages() async throws {
        let (input, writer) = try FileDescriptor.pipe()
        let (_, output) = try FileDescriptor.pipe()
        let transport = StdioTransport(input: input, output: output, logger: nil)
        try await transport.connect()

        let stream = await transport.receive()
        var iterator = stream.makeAsyncIterator()

        // First chunk ends mid-line; the second completes it and carries a full second line
        // plus the start of a third.
        try writer.writeAll(#"{"id":1,"pa"#.utf8)
        try await Task.sleep(for: .milliseconds(50))
        try writer.writeAll("rt\":\"a\"}\n{\"id\":2}\n{\"id\":".utf8)
        try await Task.sleep(for: .milliseconds(50))
        try writer.writeAll("3}\n".utf8)

        #expect(try await iterator.next() == Data(#"{"id":1,"part":"a"}"#.utf8))
        #expect(try await iterator.next() == Data(#"{"id":2}"#.utf8))
        #expect(try await iterator.next() == Data(#"{"id":3}"#.utf8))

        try writer.close()
        #expect(try await iterator.next() == nil)

        await transport.disconnect()
        try input.close()
    }

    @Test("Disconnect Finishes Idle Stream", .timeLimit(.minutes(1)))
    func testStdioTransportDisconnectFinishesStream() async throws {
        let (input, writer) = try FileDescriptor.pipe()
        let (_, output) = try FileDescriptor.pipe()
        let transport = StdioTransport(input: input, output: output, logger: nil)
        try await transport.connect()

        let stream = await transport.receive()
        let consumer = Task {
            var count = 0
            for try await _ in stream { count += 1 }
            return count
        }

        try await Task.sleep(for: .milliseconds(50))
        await transport.disconnect()
        #expect(try await consumer.value == 0)

        // The transport does not own the descriptors: they remain open for the caller.
        try writer.writeAll("x".utf8)
        try writer.close()
        try input.close()
    }

    @Test("Send Waits For Writability Under Backpressure", .timeLimit(.minutes(1)))
    func testStdioTransportSendBackpressure() async throws {
        let (reader, output) = try FileDescriptor.pipe()
        let (input, _) = try FileDescriptor.pipe()
        let transport = StdioTransport(input: input, output: output, logger: nil)
        try await transport.connect()

        // Each message exceeds the pipe buffer, so writes hit EAGAIN until the reader drains.
        let messages = (0..<4).map { index in
            Data(repeating: UInt8(ascii: "a") + UInt8(index), count: 256 * 1024)
        }
        let senders = messages.map { message in
            Task { try await transport.send(message) }
        }

        let expectedCount = messages.reduce(0) { $0 + $1.count + 1 }
        let received = try await Task.detached {
            var collected = Data()
            var buffer = [UInt8](repeating: 0, count: 64 * 1024)
            while collected.count < expectedCount {
                let count = try buffer.withUnsafeMutableBytes { try reader.read(into: $0) }
                if count == 0 { break }
                collected.append(contentsOf: buffer[..<count])
            }
            return collected
        }.value

        for sender in senders {
            try await sender.value
        }

        // Messages arrive whole (no interleaving), in some order.
        let lines = received.split(separator: UInt8(ascii: "\n")).map { Data($0) }
        #expect(lines.count == messages.count)
        #expect(Set(lines) == Set(messages))

        await transport.disconnect()
    }
}

import Dispatch
import Logging

import struct Foundation.Data

#if canImport(System)
    import System
#else
    @preconcurrency import SystemPackage
#endif

// Import for specific low-level operations not yet in Swift System
#if canImport(Darwin)
    import Darwin.POSIX
#elseif canImport(Glibc)
    import Glibc
#elseif canImport(Musl)
    import Musl
#endif

#if canImport(Darwin) || canImport(Glibc) || canImport(Musl)
    /// An implementation of the MCP stdio transport protocol.
    ///
    /// This transport implements the [stdio transport](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports#stdio)
    /// specification from the Model Context Protocol.
    ///
    /// The stdio transport works by:
    /// - Reading JSON-RPC messages from standard input
    /// - Writing JSON-RPC messages to standard output
    /// - Using newline characters as message delimiters
    /// - Supporting non-blocking I/O operations
    ///
    /// This transport is the recommended option for most MCP applications due to its
    /// simplicity and broad platform support.
    ///
    /// - Important: This transport is available on Apple platforms and Linux distributions with glibc
    ///   (Ubuntu, Debian, Fedora, CentOS, RHEL).
    ///
    /// ## Example Usage
    ///
    /// ```swift
    /// import MCP
    ///
    /// // Initialize the client
    /// let client = Client(name: "MyApp", version: "1.0.0")
    ///
    /// // Create a transport and connect
    /// let transport = StdioTransport()
    /// try await client.connect(transport: transport)
    /// ```
    public actor StdioTransport: Transport {
        private let input: FileDescriptor
        private let output: FileDescriptor
        /// Logger instance for transport-related events
        public nonisolated let logger: Logger

        private var isConnected = false
        private var reader: StdioReader?
        private var writer: StdioWriter?
        private var disconnectTask: Task<Void, Never>?
        private let messageStream: AsyncThrowingStream<Data, Swift.Error>
        private let messageContinuation: AsyncThrowingStream<Data, Swift.Error>.Continuation
        private struct DescriptorState {
            let flags: CInt
            let device: UInt64
            let inode: UInt64
        }
        private var inputState: DescriptorState?
        private var outputState: DescriptorState?

        /// Creates a new stdio transport with the specified file descriptors
        ///
        /// - Parameters:
        ///   - input: File descriptor for reading (defaults to standard input)
        ///   - output: File descriptor for writing (defaults to standard output)
        ///   - logger: Optional logger instance for transport events
        public init(
            input: FileDescriptor = FileDescriptor.standardInput,
            output: FileDescriptor = FileDescriptor.standardOutput,
            logger: Logger? = nil
        ) {
            self.input = input
            self.output = output
            self.logger =
                logger
                ?? Logger(
                    label: "mcp.transport.stdio",
                    factory: { _ in SwiftLogNoOpLogHandler() })

            // Create message stream
            var continuation: AsyncThrowingStream<Data, Swift.Error>.Continuation!
            self.messageStream = AsyncThrowingStream { continuation = $0 }
            self.messageContinuation = continuation
        }

        /// Establishes connection with the transport
        ///
        /// This method configures the file descriptors for non-blocking I/O
        /// and starts watching the input for readability.
        ///
        /// - Throws: Error if the file descriptors cannot be configured
        public func connect() async throws {
            await disconnectTask?.value
            guard !isConnected else { return }

            // Set non-blocking mode
            let originalInputState = try setNonBlocking(fileDescriptor: input)
            do {
                outputState = try setNonBlocking(fileDescriptor: output)
            } catch {
                restoreFlags(originalInputState, for: input)
                throw error
            }
            inputState = originalInputState

            writer = StdioWriter(output: output)
            reader = StdioReader(
                input: input, logger: logger, continuation: messageContinuation)
            isConnected = true
            logger.debug("Transport connected successfully")
        }

        /// Configures a file descriptor for non-blocking I/O
        ///
        /// - Parameter fileDescriptor: The file descriptor to configure
        /// - Throws: Error if the operation fails
        private func setNonBlocking(fileDescriptor: FileDescriptor) throws -> DescriptorState {
            #if canImport(Darwin) || canImport(Glibc) || canImport(Musl)
                // Get current flags
                let flags = fcntl(fileDescriptor.rawValue, F_GETFL)
                guard flags >= 0 else {
                    throw MCPError.transportError(Errno(rawValue: CInt(errno)))
                }
                var status = stat()
                guard fstat(fileDescriptor.rawValue, &status) == 0 else {
                    throw MCPError.transportError(Errno(rawValue: CInt(errno)))
                }

                // Set non-blocking flag
                let result = fcntl(fileDescriptor.rawValue, F_SETFL, flags | O_NONBLOCK)
                guard result >= 0 else {
                    throw MCPError.transportError(Errno(rawValue: CInt(errno)))
                }
                return DescriptorState(
                    flags: flags, device: UInt64(status.st_dev), inode: UInt64(status.st_ino))
            #else
                // For platforms where non-blocking operations aren't supported
                throw MCPError.internalError(
                    "Setting non-blocking mode not supported on this platform")
            #endif
        }

        private func restoreFlags(_ state: DescriptorState, for fileDescriptor: FileDescriptor) {
            guard state.flags & O_NONBLOCK == 0 else { return }
            var status = stat()
            guard fstat(fileDescriptor.rawValue, &status) == 0,
                UInt64(status.st_dev) == state.device,
                UInt64(status.st_ino) == state.inode
            else { return }
            let currentFlags = fcntl(fileDescriptor.rawValue, F_GETFL)
            guard currentFlags >= 0 else { return }
            if fcntl(fileDescriptor.rawValue, F_SETFL, currentFlags & ~O_NONBLOCK) < 0 {
                logger.warning("Failed to restore file descriptor blocking mode")
            }
        }

        /// Disconnects from the transport
        ///
        /// This stops reading, fails queued sends, and finishes the message stream.
        /// The file descriptors are left open.
        public func disconnect() async {
            if let disconnectTask {
                await disconnectTask.value
                return
            }
            guard isConnected else { return }
            isConnected = false
            let reader = self.reader
            let writer = self.writer
            self.reader = nil
            self.writer = nil
            let cleanup = Task {
                await reader?.stop()
                await writer?.close()
                if let inputState { restoreFlags(inputState, for: input) }
                if let outputState { restoreFlags(outputState, for: output) }
                inputState = nil
                outputState = nil
                messageContinuation.finish()
            }
            disconnectTask = cleanup
            await cleanup.value
            disconnectTask = nil
            logger.debug("Transport disconnected")
        }

        /// Sends a message over the transport.
        ///
        /// This method supports sending both individual JSON-RPC messages and JSON-RPC batches.
        /// Batches should be encoded as a JSON array containing multiple request/notification objects
        /// according to the JSON-RPC 2.0 specification.
        ///
        /// - Parameter message: The message data to send (without a trailing newline)
        /// - Throws: Error if the message cannot be sent
        public func send(_ message: Data) async throws {
            guard isConnected, let writer else {
                throw MCPError.transportError(Errno(rawValue: ENOTCONN))
            }
            // Add newline as delimiter
            var messageWithNewline = message
            messageWithNewline.append(UInt8(ascii: "\n"))

            try await writer.write(messageWithNewline)
        }

        /// Receives messages from the transport.
        ///
        /// Messages may be individual JSON-RPC requests, notifications, responses,
        /// or batches containing multiple requests/notifications encoded as JSON arrays.
        /// Each message is guaranteed to be a complete JSON object or array.
        ///
        /// - Returns: An AsyncThrowingStream of Data objects representing JSON-RPC messages
        public func receive() -> AsyncThrowingStream<Data, Swift.Error> {
            return messageStream
        }
    }

    /// Reads newline-delimited messages from a non-blocking descriptor when a dispatch read
    /// source reports it readable, so an idle transport causes no wakeups.
    ///
    /// Mutable state is confined to the source's queue. The descriptor is not owned and is
    /// never closed.
    private final class StdioReader: @unchecked Sendable {
        private let input: FileDescriptor
        private let logger: Logger
        private let continuation: AsyncThrowingStream<Data, Swift.Error>.Continuation
        private let queue = DispatchQueue(label: "mcp.transport.stdio.read")
        private let source: DispatchSourceRead
        private var buffer = [UInt8](repeating: 0, count: 4096)
        private var pendingData = Data()

        init(
            input: FileDescriptor,
            logger: Logger,
            continuation: AsyncThrowingStream<Data, Swift.Error>.Continuation
        ) {
            self.input = input
            self.logger = logger
            self.continuation = continuation
            self.source = DispatchSource.makeReadSource(
                fileDescriptor: input.rawValue, queue: queue)
            // The handler retains self until cancellation releases it.
            source.setEventHandler { self.readAvailable() }
            source.activate()
        }

        /// Returns once no read handler is running or can start.
        func stop() async {
            source.cancel()
            await withCheckedContinuation { continuation in
                queue.async { continuation.resume() }
            }
        }

        private func readAvailable() {
            while true {
                let bytesRead: Int
                do {
                    bytesRead = try buffer.withUnsafeMutableBytes { try input.read(into: $0) }
                } catch let error where MCPError.isResourceTemporarilyUnavailable(error) {
                    return
                } catch {
                    logger.error("Read error occurred", metadata: ["error": "\(error)"])
                    finish()
                    return
                }

                if bytesRead == 0 {
                    logger.notice("EOF received")
                    finish()
                    return
                }

                pendingData.append(contentsOf: buffer[..<bytesRead])

                while let newlineIndex = pendingData.firstIndex(of: UInt8(ascii: "\n")) {
                    let messageData = pendingData[..<newlineIndex]
                    pendingData = pendingData[(newlineIndex + 1)...]

                    if !messageData.isEmpty {
                        logger.trace(
                            "Message received", metadata: ["size": "\(messageData.count)"])
                        continuation.yield(Data(messageData))
                    }
                }
            }
        }

        private func finish() {
            continuation.finish()
            source.cancel()
        }
    }

    /// Writes messages in FIFO order to a non-blocking descriptor. On `EAGAIN` it arms a
    /// dispatch write source and resumes when the descriptor is writable, instead of polling.
    /// Whole messages are written before the next starts, so concurrent sends never interleave.
    ///
    /// Mutable state is confined to `queue`. The descriptor is not owned and is never closed.
    private final class StdioWriter: @unchecked Sendable {
        private struct Pending {
            var remaining: Data
            let continuation: CheckedContinuation<Void, Swift.Error>
        }

        private let output: FileDescriptor
        private let queue = DispatchQueue(label: "mcp.transport.stdio.write")
        private var pending: [Pending] = []
        private var writableSource: DispatchSourceWrite?
        private var isClosed = false

        init(output: FileDescriptor) {
            self.output = output
        }

        func write(_ data: Data) async throws {
            try await withCheckedThrowingContinuation { continuation in
                queue.async {
                    self.enqueue(Pending(remaining: data, continuation: continuation))
                }
            }
        }

        /// Fails queued messages and stops waiting for writability. Returns once no write is
        /// in progress or can start.
        func close() async {
            await withCheckedContinuation { (done: CheckedContinuation<Void, Never>) in
                queue.async {
                    self.closeOnQueue()
                    done.resume()
                }
            }
        }

        private func closeOnQueue() {
            isClosed = true
            writableSource?.cancel()
            writableSource = nil
            let failed = pending
            pending = []
            for item in failed {
                item.continuation.resume(
                    throwing: MCPError.transportError(Errno(rawValue: ENOTCONN)))
            }
        }

        private func enqueue(_ item: Pending) {
            guard !isClosed else {
                item.continuation.resume(
                    throwing: MCPError.transportError(Errno(rawValue: ENOTCONN)))
                return
            }
            pending.append(item)
            if writableSource == nil {
                drain()
            }
        }

        private func drain() {
            while !pending.isEmpty {
                do {
                    let written = try pending[0].remaining.withUnsafeBytes {
                        try output.write($0)
                    }
                    pending[0].remaining = pending[0].remaining.dropFirst(written)
                    if pending[0].remaining.isEmpty {
                        pending.removeFirst().continuation.resume()
                    }
                } catch let error where MCPError.isResourceTemporarilyUnavailable(error) {
                    awaitWritable()
                    return
                } catch {
                    pending.removeFirst().continuation.resume(
                        throwing: MCPError.transportError(error))
                }
            }
        }

        private func awaitWritable() {
            let source = DispatchSource.makeWriteSource(
                fileDescriptor: output.rawValue, queue: queue)
            source.setEventHandler {
                source.cancel()
                self.writableSource = nil
                self.drain()
            }
            writableSource = source
            source.activate()
        }
    }
#endif

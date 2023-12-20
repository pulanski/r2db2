# Database Wire Protocol Design

## Table of Contents

<!-- TOC tocDepth:2..3 chapterDepth:2..6 -->

- [Database Wire Protocol Design](#database-wire-protocol-design)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
    - [Key Features](#key-features)
  - [Protocol Lifecycle](#protocol-lifecycle)
    - [Connection Establishment](#connection-establishment)
    - [Command Execution](#command-execution)
    - [Connection Termination](#connection-termination)
  - [Message Types](#message-types)
  - [Examples: Protocol in Action](#examples-protocol-in-action)
    - [Connection Setup with Authentication](#connection-setup-with-authentication)
    - [Query Execution and Response](#query-execution-and-response)
    - [Connection Termination (Client)](#connection-termination-client)
  - [Message Formats](#message-formats)
    - [Common Structure](#common-structure)
    - [Specific Message Formats](#specific-message-formats)
      - [StartupMessage](#startupmessage)
      - [AuthenticationRequest](#authenticationrequest)
      - [QueryMessage](#querymessage)
      - [DataRowMessage](#datarowmessage)
      - [CommandCompleteMessage](#commandcompletemessage)
      - [ErrorResponse](#errorresponse)
  - [Authentication Methods](#authentication-methods)
    - [Password-Based Authentication](#password-based-authentication)
    - [Token-Based Authentication](#token-based-authentication)
    - [Certificate-Based Authentication](#certificate-based-authentication)
  - [Security Considerations and Error Handling](#security-considerations-and-error-handling)
  - [Different Modes of Operation](#different-modes-of-operation)
  - [Unauthenticated Version (Default Mode)](#unauthenticated-version-default-mode)
    - [Characteristics](#characteristics)
    - [Protocol Flow](#protocol-flow)
  - [Authenticated Version](#authenticated-version)
    - [Characteristics (Authenticated)](#characteristics-authenticated)
      - [Protocol Flow (Authenticated)](#protocol-flow-authenticated)
  - [Choosing Between the Two Modes](#choosing-between-the-two-modes)
  - [Authentication Methods Supported](#authentication-methods-supported)
    - [Password-Based](#password-based)
      - [Default Credentials](#default-credentials)
  - [Compression](#compression)
    - [Compression Strategy](#compression-strategy)
      - [Example: Compression Negotiation](#example-compression-negotiation)
    - [Handling Compression](#handling-compression)
  - [Monitoring and Diagnostics](#monitoring-and-diagnostics)
    - [Monitoring Features](#monitoring-features)
    - [Diagnostic Capabilities](#diagnostic-capabilities)
    - [Differences from PostgreSQL Wire Protocol](#differences-from-postgresql-wire-protocol)
  - [Future Enhancements](#future-enhancements)
  - [References](#references)

<!-- /TOC -->

This document outlines the design of **r2db2**'s database wire protocol inspired by the PostgreSQL wire protocol. The protocol facilitates secure, efficient communication between a client and a database server. It handles authentication, SSL encryption, command execution, and retries.

## Overview

The protocol operates over a TCP/IP connection and is structured around a request-response model. It supports various message types, including authentication requests, query execution, and result transmission.

### Key Features

- **SSL Encryption**: Ensures secure data transmission.
- **Authentication**: Verifies user credentials.
- **Command Execution**: Allows clients to execute database commands.
- **Retries**: Handles transient errors with automatic retries.
- **Extensibility**: Allows for future enhancements.

## Protocol Lifecycle

### Connection Establishment

1. **SSL Handshake**:
   - The client initiates an SSL handshake if encryption is required.
   - The server responds with SSL handshake acknowledgment.

2. **Authentication**:
   - The client sends a `StartupMessage` with credentials.
   - The server responds with an `AuthenticationRequest`.

3. **Connection Confirmation**:
   - Upon successful authentication, the server sends a `ReadyForQuery` message.

### Command Execution

1. **Command Submission**:
   - The client sends a `QueryMessage` containing the SQL command.

2. **Query Processing**:
   - The server processes the query and sends a `CommandCompleteMessage` upon successful execution or an `ErrorResponse` in case of failure.

3. **Data Transmission** (if applicable):
   - For queries returning data (e.g., SELECT), the server sends `DataRowMessages` followed by a `CommandCompleteMessage`.

### Connection Termination

- The client sends a `TerminationMessage` to close the connection.

## Message Types

- `StartupMessage`: Initiates the connection and includes authentication data.
- `AuthenticationRequest`: Requests client authentication.
- `ReadyForQuery`: Indicates the server is ready to receive commands.
- `QueryMessage`: Contains an SQL command to be executed.
- `CommandCompleteMessage`: Indicates successful execution of a command.
- `ErrorResponse`: Indicates an error.
- `DataRowMessage`: Contains a row of query results.
- `TerminationMessage`: Indicates the client wishes to close the connection.

## Examples: Protocol in Action

### Connection Setup with Authentication

```plaintext
  +--------+                                           +--------+
  | Client |                                           | Server |
  +--------+                                           +--------+
      |                                                     |
      | (1) SSL Handshake                                   |
      |---------------------------------------------------->|
      |                                                     |
      | (2) SSL Handshake Acknowledgment                    |
      |<----------------------------------------------------|
      |                                                     |
      | (3) StartupMessage with Credentials                 |
      |---------------------------------------------------->|
      |                                                     |
      | (4) AuthenticationRequest                           |
      |<----------------------------------------------------|
      |                                                     |
      | (5) Authentication (password)                       |
      |---------------------------------------------------->|
      |                                                     |
      | (6) ReadyForQuery                                   |
      |<----------------------------------------------------|
      |                                                     |
```

### Query Execution and Response

```plaintext
  +--------+                                           +--------+
  | Client |                                           | Server |
  +--------+                                           +--------+
      |                                                     |
      | (1) QueryMessage ("SELECT * FROM table")            |
      |---------------------------------------------------->|
      |                                                     |
      |                   Processing Query...               |
      |                                                     |
      | (2) DataRowMessage (row 1)                          |
      |<----------------------------------------------------|
      | (3) DataRowMessage (row 2)                          |
      |<----------------------------------------------------|
      |                                                     |
      | (4) CommandCompleteMessage                          |
      |<----------------------------------------------------|
      |                                                     |
```

### Connection Termination (Client)

```plaintext
  +--------+                                           +--------+
  | Client |                                           | Server |
  +--------+                                           +--------+
      |                                                     |
      | (1) TerminationMessage                              |
      |---------------------------------------------------->|
      |                                                     |
```

## Message Formats

Each message in the protocol follows a specific format, designed for efficient parsing and processing.

- [Common Structure](#common-structure)
- [Specific Message Formats](#specific-message-formats)
  - [StartupMessage](#startupmessage)
  - [AuthenticationRequest](#authenticationrequest)
  - [QueryMessage](#querymessage)
  - [DataRowMessage](#datarowmessage)
  - [CommandCompleteMessage](#commandcompletemessage)
  - [ErrorResponse](#errorresponse)

### Common Structure

All messages have a common header structure:

- **Type**: 1 byte identifying the message type.
- **Length**: 4 bytes (Int32) representing the length of the message, including the header (**maximum 2GB**).

```plaintext
| 1 byte | 4 bytes | Length (- 5 for header) bytes |
| ------ | ------- | ----------------------------- |
| Type   | Length  | Payload                       |
```

### Specific Message Formats

#### StartupMessage

```plaintext
Byte1('S') - Identifies the message as a StartupMessage.
Int32 - Length of the message in bytes, including the header.
String - Username.
String - Password (optional, for authenticated mode).
```

#### AuthenticationRequest

```plaintext
Byte1('A') - Identifies the message as an AuthenticationRequest.
Int32 - Length of the message in bytes, including the header.
Byte1 - Authentication type (0 for none, 1 for password, etc.).
```

#### QueryMessage

```plaintext
Byte1('Q') - Identifies the message as a QueryMessage.
Int32 - Length of the message in bytes, including the header.
String - SQL query text.
```

#### DataRowMessage

```plaintext
Byte1('D') - Identifies the message as a DataRowMessage.
Int32 - Length of the message in bytes, including the header.
Int32 - Number of columns in the row.
ForEach Column:
    Int32 - Length of the column value.
    ByteN - Column value.
```

#### CommandCompleteMessage

```plaintext
Byte1('C') - Identifies the message as a CommandCompleteMessage.
Int32 - Length of the message in bytes, including the header.
String - Status tag (e.g., "SELECT 3").
```

#### ErrorResponse

```plaintext
Byte1('E') - Identifies the message as an ErrorResponse.
Int32 - Length of the message in bytes, including the header.
String - Error message.
```

## Authentication Methods

### Password-Based Authentication

- **Procedure**:
  - The client sends a `StartupMessage` with the username and password.
  - The server verifies the credentials against its user database (e.g., LDAP, Active Directory, etc.).

### Token-Based Authentication

- **Procedure**:
  - The client sends a `StartupMessage` with a secure token.
  - The server validates the token with a token service or internal validation mechanism.

### Certificate-Based Authentication

- **Procedure**:
  - Utilizes SSL certificates.
  - The client presents its certificate as part of the SSL handshake.
  - The server verifies the client's certificate against a trusted certificate authority.

## Security Considerations and Error Handling

- **SSL Encryption**: Ensures all data transmitted is encrypted (_if enabled_).
- **Authentication**: Robust authentication mechanism to verify user identity and prevent unauthorized access.
- **Error Handling**: Clear and descriptive error messages for client-side handling.
- **Retries**: In-built retry mechanism for handling transient network errors.
- **Data Validation**: Rigorous validation of all incoming data to prevent SQL injection and other forms of attacks.
- **Error Handling**: Careful handling of errors to avoid revealing sensitive information about the database structure or internal workings.

## Different Modes of Operation

The protocol supports two modes of operation: **unauthenticated** and **authenticated**. The mode is determined by the server based on the configuration. The client can also specify the mode in the `StartupMessage`. Note that the server can reject the client's request to change the mode (e.g., if the client wishes to use the unauthenticated mode but the server requires authentication).

- **Unauthenticated Mode**: The client does not send any authentication data in the `StartupMessage`. The server allows the connection without verifying the client's identity.
- **Authenticated Mode**: The client sends authentication data in the `StartupMessage`. The server challenges the client for authentication before allowing the connection.

## Unauthenticated Version (Default Mode)

In the unauthenticated version, the protocol does not involve any form of user identity verification. This mode is typically chosen for simplicity and ease of use, especially in development environments or internal networks where security is less of a concern.

### Characteristics

1. **No Credential Exchange**: The startup message from the client does not include any authentication data like usernames or passwords.

2. **Simplified Connection Process**: The server allows connections without validating the client’s identity, reducing the number of steps in the connection establishment process.

3. **Limited Access Control**: There's little to no control over who can access the database, making it suitable for environments where access is unrestricted.

4. **Potential Security Risks**: Without authentication, the server is vulnerable to unauthorized access, making it unsuitable for production environments with sensitive data.

### Protocol Flow

```plaintext
Client                                Server
  |                                     |
  |--- StartupMessage ----------------->|
  |                                     |
  |<-- ReadyForQuery (No Authentication)|
  |                                     |
```

## Authenticated Version

In the authenticated version, the protocol includes steps to verify the identity of the client. This mode is essential for production environments or wherever data security and access control are important.

### Characteristics (Authenticated)

1. **Credential Exchange**: The client sends credentials (username/password or tokens) as part of the `StartupMessage`.

2. **Authentication Process**: The server challenges the client for authentication, often using methods like password, token, or certificate-based authentication.

3. **Enhanced Security**: By validating the identity of each client, the server ensures that only authorized users can access the database.

4. **Complex Connection Setup**: The connection establishment process involves additional steps for authentication, making it slightly more complex than the unauthenticated version.

#### Protocol Flow (Authenticated)

```plaintext
Client                                Server
  |                                     |
  |--- StartupMessage (credentials) --->|
  |                                     |
  |<-- AuthenticationRequest -----------|
  |                                     |
  |--- Authentication Response -------->|
  |                                     |
  |<-- ReadyForQuery (Authenticated) ---|
  |                                     |
```

## Choosing Between the Two Modes

- **Development vs Production**: Unauthenticated mode is often suitable for development or internal testing environments, while authenticated mode is crucial for production environments.

- **Security Needs**: If the data is sensitive or the server is exposed to public networks, authentication is necessary.

- **Access Control**: If the server needs to restrict access to certain users, authentication is necessary.

## Authentication Methods Supported

The protocol supports various authentication methods, including password-based, token-based, and certificate-based authentication. The server can support multiple authentication methods, and the client can choose the method to use in the `StartupMessage`.

### Password-Based

In password-based authentication, the client sends a username and password in the `StartupMessage`. The server verifies the credentials and allows the connection if they are valid.

#### Default Credentials

```plaintext
username: test
password: test
```

## Compression

In modern networked applications, data transmission can be a bottleneck, especially when dealing with large datasets. Compression is a technique to reduce the size of the data transmitted over the network, thereby increasing the efficiency and reducing latency.

### Compression Strategy

- **Selective Compression**: Not all data benefits equally from compression (e.g., binary data). The protocol should selectively compress data based on type and size.

- **Compression Algorithms**: Implement widely-used and efficient compression algorithms. Candidates include:
  - **LZ4**: Fast compression and decompression speeds.
  - **Snappy**: Optimized for speed and reasonable compression ratios.
  - **Zstandard**: Offers a balance between speed and compression ratio.

For more information, see [Comparison of Data Compression Algorithms](https://en.wikipedia.org/wiki/Comparison_of_data_compression_algorithms).

In our implementation, we use the [LZ4](https://lz4.github.io/lz4/) compression algorithm for its speed and reasonable compression ratios (see [LZ4 Compression Benchmarks](https://lz4.github.io/lz4/)) at the expense of slightly higher CPU usage.

- **Negotiating Compression**: During the connection setup, the client and server negotiate the use and type of compression. This can be part of the `StartupMessage` and `ReadyForQuery` handshake.

#### Example: Compression Negotiation

```plaintext
Client                                Server
  |                                     |
  |--- StartupMessage (with LZ4) ------>|
  |                                     |
  |<-- ReadyForQuery (LZ4 Accepted) ----|
  |                                     |
```

### Handling Compression

- **Compression Level**: The protocol should allow specifying the compression level, balancing between compression ratio and performance.

- **On-the-fly Compression/Decompression**: Implement compression/decompression in the protocol stack so that it's transparent to the application layer.

## Monitoring and Diagnostics

Effective monitoring and diagnostics are crucial for maintaining the health and performance of the database system. The protocol should facilitate easy monitoring and offer diagnostic capabilities.

### Monitoring Features

- **Connection Health**: Periodic heartbeats or similar mechanisms to ensure that connections are alive and healthy.

- **Performance Metrics**: Tracking key performance metrics like query response times, number of active connections, and data throughput.

- **Error Tracking**: Detailed error reports for failed queries or protocol violations, aiding in quick resolution of issues.

### Diagnostic Capabilities

- **Logging**: Detailed logging of all protocol interactions, including connection setups, queries, and disconnections.
- **Trace Mode**: A mode where each step in the query processing and data transmission is logged for deep analysis.
- **Accessible Metrics**: Metrics accessible through standard APIs or endpoints, allowing for real-time monitoring and alerting.

### Differences from PostgreSQL Wire Protocol

While inspired by the PostgreSQL wire protocol, this protocol introduces several variations:

1. **Simplified Message Types**: Reduced and more straightforward message types for ease of implementation and understanding.

2. **Unified Authentication Request**: A single authentication request message that can handle different authentication methods.

3. **Extensible Authentication**: Designed to easily incorporate new authentication methods as they emerge.

4. **Enhanced Error Reportingg and Diagnostics**: More detailed error reporting in the `ErrorResponse` to aid in client-side diagnostics and troubleshooting.

## Future Enhancements

- **Load Balancing and Failover**: Support for load balancing and automatic failover to enhance availability and performance.

- **Monitoring and Diagnostics**: Enhanced features for monitoring the health of the database connection and diagnosing issues.

## References

- [PostgreSQL Wire Protocol](https://www.postgresql.org/docs/13/protocol.html)
- [PostgreSQL Startup Message](https://www.postgresql.org/docs/13/protocol-message-formats.html)
- [PostgreSQL Authentication](<https://www.postgresql.org/docs/13/protocol-flow.html#id->)

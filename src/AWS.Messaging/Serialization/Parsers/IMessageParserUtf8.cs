// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System;
using System.Buffers;
using System.Text;
using System.Text.Json;
using Amazon.SQS.Model;

namespace AWS.Messaging.Serialization.Parsers;

/// <summary>
/// Reader-based parser contract for detecting and extracting inner envelope and metadata from an outer wrapper.
/// Implementations should parse from UTF-8 bytes and return a slice for the inner message when matched.
/// </summary>
internal interface IMessageParserUtf8
{
    /// <summary>
    /// Attempts to parse the given payload as a specific outer wrapper (e.g. SNS, EventBridge).
    /// On success, returns the inner message payload and aggregated metadata.
    /// </summary>
    /// <param name="utf8Payload">The input payload as UTF-8 bytes (memory-backed to allow zero-copy slicing).</param>
    /// <param name="originalMessage">The original SQS message for harvesting SQS metadata.</param>
    /// <param name="innerPayload">The extracted inner payload as UTF-8 bytes.</param>
    /// <param name="metadata">The extracted metadata (implementation may include SQS metadata).</param>
    /// <returns>True if this parser recognized and parsed the payload; otherwise false.</returns>
    bool TryParse(ReadOnlyMemory<byte> utf8Payload, Message originalMessage, out ReadOnlyMemory<byte> innerPayload, out MessageMetadata metadata);
}

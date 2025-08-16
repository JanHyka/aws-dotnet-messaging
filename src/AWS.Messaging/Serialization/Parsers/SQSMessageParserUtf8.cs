// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System.Text.Json;
using Amazon.SQS.Model;
using AWS.Messaging.Serialization.Handlers;
using AWS.Messaging.Serialization.Helpers;

namespace AWS.Messaging.Serialization.Parsers;

internal sealed class SQSMessageParserUtf8 : IMessageParserUtf8
{
    public bool TryParse(ReadOnlyMemory<byte> utf8Payload, Message originalMessage, ArrayPoolScope pool, out ReadOnlyMemory<byte> innerPayload, out MessageMetadata metadata)
    {
        // SQS fallback: always matches and returns original payload and SQS metadata with zero-copy slice.
        innerPayload = utf8Payload;
        metadata = new MessageMetadata
        {
            SQSMetadata = MessageMetadataHandler.CreateSQSMetadata(originalMessage)
        };
        return true;
    }
}

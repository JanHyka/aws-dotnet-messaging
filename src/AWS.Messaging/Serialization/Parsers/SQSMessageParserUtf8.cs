// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using System.Text.Json;
using Amazon.SQS.Model;
using AWS.Messaging.Serialization.Handlers;

namespace AWS.Messaging.Serialization.Parsers;

internal sealed class SQSMessageParserUtf8 : IMessageParserUtf8
{
    public bool TryParse(ReadOnlySpan<byte> utf8Payload, Message originalMessage, out ReadOnlyMemory<byte> innerPayload, out MessageMetadata metadata)
    {
        // SQS fallback: always matches and returns original payload and SQS metadata.
        innerPayload = new ReadOnlyMemory<byte>(utf8Payload.ToArray());
        metadata = new MessageMetadata
        {
            SQSMetadata = MessageMetadataHandler.CreateSQSMetadata(originalMessage)
        };
        return true;
    }
}

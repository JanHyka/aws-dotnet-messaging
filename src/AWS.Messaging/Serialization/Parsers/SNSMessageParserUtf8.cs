// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System;
using System.Buffers;
using System.Text;
using System.Text.Json;
using Amazon.SQS.Model;
using AWS.Messaging.Internal;
using AWS.Messaging.Serialization.Handlers;

namespace AWS.Messaging.Serialization.Parsers;

internal sealed class SNSMessageParserUtf8 : IMessageParserUtf8
{
    public bool TryParse(ReadOnlySpan<byte> utf8Payload, Message originalMessage, out ReadOnlyMemory<byte> innerPayload, out MessageMetadata metadata)
    {
        innerPayload = default;
        metadata = default!;

        var reader = new Utf8JsonReader(utf8Payload, isFinalBlock: true, state: default);
        if (reader.TokenType == JsonTokenType.None && !reader.Read())
            return false;
        if (reader.TokenType != JsonTokenType.StartObject)
            return false;

        // Track expected fields: Type == "Notification", TopicArn, MessageId, Message
        string? typeValue = null;
        string? topicArn = null;
        string? messageId = null;
        ReadOnlyMemory<byte> messageBytes = default;
        Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>? messageAttributes = null;
        DateTimeOffset? timestamp = null;
        string? subject = null;
        string? unsubscribeUrl = null;

        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
                break;
            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                reader.Skip();
                continue;
            }

            var name = reader.GetString();
            if (!reader.Read()) break;

            switch (name)
            {
                case "Type":
                    typeValue = reader.TokenType == JsonTokenType.String ? reader.GetString() : null;
                    break;
                case "TopicArn":
                    topicArn = reader.TokenType == JsonTokenType.String ? reader.GetString() : null;
                    break;
                case "MessageId":
                    messageId = reader.TokenType == JsonTokenType.String ? reader.GetString() : null;
                    break;
                case "Timestamp":
                    if (reader.TokenType == JsonTokenType.String && reader.TryGetDateTimeOffset(out var ts)) timestamp = ts;
                    break;
                case "Subject":
                    subject = reader.TokenType == JsonTokenType.String ? reader.GetString() : null;
                    break;
                case "UnsubscribeURL":
                    unsubscribeUrl = reader.TokenType == JsonTokenType.String ? reader.GetString() : null;
                    break;
                case "MessageAttributes":
                    if (reader.TokenType == JsonTokenType.StartObject)
                    {
                        try
                        {
                            var dict = JsonSerializer.Deserialize(ref reader, MessagingJsonSerializerContext.Default.DictionarySNSMessageAttributeValue);
                            messageAttributes = dict;
                        }
                        catch
                        {
                            reader.Skip();
                        }
                    }
                    else
                    {
                        reader.Skip();
                    }
                    break;
                case "Message":
                    if (reader.TokenType == JsonTokenType.String)
                    {
                        var inner = reader.GetString();
                        if (string.IsNullOrEmpty(inner)) return false;
                        var bytes = Encoding.UTF8.GetBytes(inner);
                        messageBytes = new ReadOnlyMemory<byte>(bytes);
                    }
                    else if (reader.TokenType == JsonTokenType.StartObject || reader.TokenType == JsonTokenType.StartArray)
                    {
                        // Some SNS deliveries can contain JSON object in Message; capture exact slice
                        var start = (int)reader.TokenStartIndex;
                        reader.Skip();
                        var end = (int)reader.BytesConsumed;
                        messageBytes = new ReadOnlyMemory<byte>(utf8Payload.Slice(start, end - start).ToArray());
                    }
                    else
                    {
                        reader.Skip();
                    }
                    break;
                default:
                    reader.Skip();
                    break;
            }
        }

        if (!string.Equals(typeValue, "Notification", StringComparison.Ordinal) || topicArn is null || messageId is null || messageBytes.IsEmpty)
        {
            return false;
        }

        metadata = new MessageMetadata
        {
            SQSMetadata = MessageMetadataHandler.CreateSQSMetadata(originalMessage),
            SNSMetadata = new SNSMetadata
            {
                TopicArn = topicArn,
                MessageId = messageId,
                Subject = subject,
                UnsubscribeURL = unsubscribeUrl,
                Timestamp = timestamp ?? default,
                MessageAttributes = messageAttributes
            }
        };
        innerPayload = messageBytes;
        return true;
    }
}

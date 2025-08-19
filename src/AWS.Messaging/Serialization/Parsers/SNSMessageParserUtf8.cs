// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using System.Text.Json;
using Amazon.SQS.Model;
using AWS.Messaging.Internal;
using AWS.Messaging.Serialization.Handlers;
using AWS.Messaging.Serialization.Helpers;

namespace AWS.Messaging.Serialization.Parsers;

internal sealed class SNSMessageParserUtf8 : IMessageParserUtf8
{
    private static readonly byte[] s_tokenTypeNotification = Encoding.UTF8.GetBytes("\"Type\":\"Notification\"");
    private static readonly byte[] s_tokenTopicArn = Encoding.UTF8.GetBytes("\"TopicArn\"");

    // Property name tokens
    private static readonly byte[] s_propType = Encoding.UTF8.GetBytes("Type");
    private static readonly byte[] s_propTopicArn = Encoding.UTF8.GetBytes("TopicArn");
    private static readonly byte[] s_propMessageId = Encoding.UTF8.GetBytes("MessageId");
    private static readonly byte[] s_propTimestamp = Encoding.UTF8.GetBytes("Timestamp");
    private static readonly byte[] s_propSubject = Encoding.UTF8.GetBytes("Subject");
    private static readonly byte[] s_propUnsubscribeUrl = Encoding.UTF8.GetBytes("UnsubscribeURL");
    private static readonly byte[] s_propMessageAttributes = Encoding.UTF8.GetBytes("MessageAttributes");
    private static readonly byte[] s_propMessage = Encoding.UTF8.GetBytes("Message");

    public bool QuickMatch(ReadOnlySpan<byte> utf8Payload)
    {
        var span = utf8Payload.Length <= 2048 ? utf8Payload : utf8Payload.Slice(0, 2048);
        return span.IndexOf(s_tokenTypeNotification) >= 0 || span.IndexOf(s_tokenTopicArn) >= 0;
    }

    public bool TryParse(ReadOnlyMemory<byte> utf8Payload, Message originalMessage, ArrayPoolScope pool, out ReadOnlyMemory<byte> innerPayload, out MessageMetadata metadata)
    {
        innerPayload = default;
        metadata = default!;

        var reader = new Utf8JsonReader(utf8Payload.Span, isFinalBlock: true, state: default);
        if (reader.TokenType == JsonTokenType.None && !reader.Read())
            return false;
        if (reader.TokenType != JsonTokenType.StartObject)
            return false;

        string? typeValue = null;
        string? topicArn = null;
        string? messageId = null;
        ReadOnlyMemory<byte> messageBytes = default;
        Dictionary<string, Amazon.SimpleNotificationService.Model.MessageAttributeValue>? messageAttributes = null;
        DateTimeOffset? timestamp = null;
        string? subject = null;
        string? unsubscribeUrl = null;

        bool hasType = false, hasTopicArn = false, hasMessageId = false;

        try
        {
            while (reader.Read())
            {
                if (reader.TokenType == JsonTokenType.EndObject)
                    break;
                if (reader.TokenType != JsonTokenType.PropertyName)
                {
                    reader.Skip();
                    continue;
                }

                if (reader.ValueTextEquals(s_propType))
                {
                    reader.Read();
                    typeValue = reader.GetString();
                    hasType = true;
                    continue;
                }
                if (reader.ValueTextEquals(s_propTopicArn))
                {
                    reader.Read();
                    topicArn = reader.GetString();
                    hasTopicArn = true;
                    continue;
                }
                if (reader.ValueTextEquals(s_propMessageId))
                {
                    reader.Read();
                    messageId = reader.GetString();
                    hasMessageId = true;
                    continue;
                }
                if (reader.ValueTextEquals(s_propTimestamp))
                {
                    reader.Read();
                    timestamp = reader.GetDateTimeOffset();
                    continue;
                }
                if (reader.ValueTextEquals(s_propSubject))
                {
                    reader.Read();
                    subject = reader.GetString();
                    continue;
                }
                if (reader.ValueTextEquals(s_propUnsubscribeUrl))
                {
                    reader.Read();
                    unsubscribeUrl = reader.GetString();
                    continue;
                }
                if (reader.ValueTextEquals(s_propMessageAttributes))
                {
                    reader.Read();
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
                        throw new JsonException("Invalid 'MessageAttributes' token");
                    }
                    continue;
                }
                if (reader.ValueTextEquals(s_propMessage))
                {
                    reader.Read();
                    if (reader.TokenType == JsonTokenType.String)
                    {
                        messageBytes = Utf8JsonReaderHelper.UnescapeValue(ref reader, pool);
                    }
                    else if (reader.TokenType == JsonTokenType.StartObject || reader.TokenType == JsonTokenType.StartArray)
                    {
                        var start = (int)reader.TokenStartIndex;
                        reader.Skip();
                        var end = (int)reader.BytesConsumed;
                        messageBytes = utf8Payload.Slice(start, end - start);
                    }
                    else
                    {
                        throw new JsonException("Invalid 'Message' token");
                    }
                    continue;
                }

                // Unknown property: skip
                reader.Skip();
            }
        }
        catch (Exception ex) when (ex is JsonException || ex is InvalidOperationException || ex is FormatException)
        {
            return false;
        }

        if (!(hasType && string.Equals(typeValue, "Notification", StringComparison.Ordinal) && hasTopicArn && hasMessageId) || messageBytes.IsEmpty)
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

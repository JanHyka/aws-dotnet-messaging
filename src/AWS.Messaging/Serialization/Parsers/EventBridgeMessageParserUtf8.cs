// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using System.Text.Json;
using Amazon.SQS.Model;
using AWS.Messaging.Serialization.Handlers;
using AWS.Messaging.Serialization.Helpers;

namespace AWS.Messaging.Serialization.Parsers;

internal sealed class EventBridgeMessageParserUtf8 : IMessageParserUtf8
{
    private static readonly byte[] s_tokenDetailType = Encoding.UTF8.GetBytes("\"detail-type\"");
    private static readonly byte[] s_tokenDetail = Encoding.UTF8.GetBytes("\"detail\"");

    public bool QuickMatch(ReadOnlySpan<byte> utf8Payload)
    {
        var span = utf8Payload.Length <= 2048 ? utf8Payload : utf8Payload.Slice(0, 2048);
        return span.IndexOf(s_tokenDetailType) >= 0 && span.IndexOf(s_tokenDetail) >= 0;
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

        bool hasDetail = false, hasDetailType = false, hasSource = false, hasTime = false;
        ReadOnlyMemory<byte> detailBytes = default;
        string? id = null, source = null, account = null, region = null, detailType = null;
        DateTimeOffset time = default;
        List<string>? resources = null;

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

                var name = reader.GetString();
                if (!reader.Read()) break;

                switch (name)
                {
                    case "detail":
                        hasDetail = true;
                        if (reader.TokenType == JsonTokenType.StartObject || reader.TokenType == JsonTokenType.StartArray)
                        {
                            var start = (int)reader.TokenStartIndex;
                            reader.Skip();
                            var end = (int)reader.BytesConsumed;
                            detailBytes = utf8Payload.Slice(start, end - start);
                        }
                        else if (reader.TokenType == JsonTokenType.String)
                        {
                            detailBytes = Utf8JsonReaderHelper.UnescapeValue(ref reader, pool);
                        }
                        else
                        {
                            // unexpected token for detail -> fail fast
                            throw new JsonException("Invalid 'detail' token");
                        }
                        break;
                    case "detail-type":
                        detailType = reader.GetString();
                        hasDetailType = true;
                        break;
                    case "source":
                        source = reader.GetString();
                        hasSource = true;
                        break;
                    case "time":
                        time = reader.GetDateTimeOffset();
                        hasTime = true;
                        break;
                    case "id":
                        id = reader.GetString();
                        break;
                    case "account":
                        account = reader.GetString();
                        break;
                    case "region":
                        region = reader.GetString();
                        break;
                    case "resources":
                        if (reader.TokenType == JsonTokenType.StartArray)
                        {
                            resources = new List<string>();
                            while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                            {
                                // Let GetString throw for invalid tokens
                                var v = reader.GetString();
                                if (!string.IsNullOrEmpty(v)) resources.Add(v);
                            }
                        }
                        else
                        {
                            // unexpected token for resources -> fail fast
                            throw new JsonException("Invalid 'resources' token");
                        }
                        break;
                    default:
                        reader.Skip();
                        break;
                }
            }
        }
        catch (Exception ex) when (ex is JsonException || ex is System.InvalidOperationException || ex is System.FormatException)
        {
            // TryParse contract: no exceptions on failure
            return false;
        }

        if (!(hasDetail && hasDetailType && hasSource && hasTime) || detailBytes.IsEmpty)
            return false;

        metadata = new MessageMetadata
        {
            SQSMetadata = MessageMetadataHandler.CreateSQSMetadata(originalMessage),
            EventBridgeMetadata = new EventBridgeMetadata
            {
                EventId = id,
                DetailType = detailType,
                Source = source,
                Time = time,
                AWSAccount = account,
                AWSRegion = region,
                Resources = resources
            }
        };
        innerPayload = detailBytes;
        return true;
    }
}

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

    // Property name tokens
    private static readonly byte[] s_propDetail = Encoding.UTF8.GetBytes("detail");
    private static readonly byte[] s_propDetailTypeName = Encoding.UTF8.GetBytes("detail-type");
    private static readonly byte[] s_propSource = Encoding.UTF8.GetBytes("source");
    private static readonly byte[] s_propTime = Encoding.UTF8.GetBytes("time");
    private static readonly byte[] s_propId = Encoding.UTF8.GetBytes("id");
    private static readonly byte[] s_propAccount = Encoding.UTF8.GetBytes("account");
    private static readonly byte[] s_propRegion = Encoding.UTF8.GetBytes("region");
    private static readonly byte[] s_propResources = Encoding.UTF8.GetBytes("resources");

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

                if (reader.ValueTextEquals(s_propDetail))
                {
                    reader.Read();
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
                        throw new JsonException("Invalid 'detail' token");
                    }
                    continue;
                }

                if (reader.ValueTextEquals(s_propDetailTypeName))
                {
                    reader.Read();
                    detailType = reader.GetString();
                    hasDetailType = true;
                    continue;
                }
                if (reader.ValueTextEquals(s_propSource))
                {
                    reader.Read();
                    source = reader.GetString();
                    hasSource = true;
                    continue;
                }
                if (reader.ValueTextEquals(s_propTime))
                {
                    reader.Read();
                    time = reader.GetDateTimeOffset();
                    hasTime = true;
                    continue;
                }
                if (reader.ValueTextEquals(s_propId))
                {
                    reader.Read();
                    id = reader.GetString();
                    continue;
                }
                if (reader.ValueTextEquals(s_propAccount))
                {
                    reader.Read();
                    account = reader.GetString();
                    continue;
                }
                if (reader.ValueTextEquals(s_propRegion))
                {
                    reader.Read();
                    region = reader.GetString();
                    continue;
                }
                if (reader.ValueTextEquals(s_propResources))
                {
                    reader.Read();
                    if (reader.TokenType == JsonTokenType.StartArray)
                    {
                        resources = new List<string>();
                        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        {
                            var v = reader.GetString();
                            if (!string.IsNullOrEmpty(v)) resources.Add(v);
                        }
                    }
                    else
                    {
                        throw new JsonException("Invalid 'resources' token");
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

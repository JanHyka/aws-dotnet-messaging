// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System.Text;
using System.Text.Json;
using Amazon.SQS.Model;
using AWS.Messaging.Serialization.Handlers;

namespace AWS.Messaging.Serialization.Parsers;

internal sealed class EventBridgeMessageParserUtf8 : IMessageParserUtf8
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

        bool hasDetail = false, hasDetailType = false, hasSource = false, hasTime = false;
        ReadOnlyMemory<byte> detailBytes = default;
        string? id = null, source = null, account = null, region = null, detailType = null;
        DateTimeOffset? time = null;
        List<string>? resources = null;

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
                        detailBytes = new ReadOnlyMemory<byte>(utf8Payload.Slice(start, end - start).ToArray());
                    }
                    else if (reader.TokenType == JsonTokenType.String)
                    {
                        var detail = reader.GetString();
                        if (detail is null) return false;
                        detailBytes = new ReadOnlyMemory<byte>(Encoding.UTF8.GetBytes(detail));
                    }
                    else
                    {
                        reader.Skip();
                    }
                    break;
                case "detail-type":
                    hasDetailType = reader.TokenType == JsonTokenType.String;
                    if (hasDetailType) detailType = reader.GetString();
                    break;
                case "source":
                    hasSource = reader.TokenType == JsonTokenType.String;
                    if (hasSource) source = reader.GetString();
                    break;
                case "time":
                    if (reader.TokenType == JsonTokenType.String && reader.TryGetDateTimeOffset(out var dto))
                    {
                        time = dto;
                    }
                    break;
                case "id":
                    if (reader.TokenType == JsonTokenType.String) id = reader.GetString();
                    break;
                case "account":
                    if (reader.TokenType == JsonTokenType.String) account = reader.GetString();
                    break;
                case "region":
                    if (reader.TokenType == JsonTokenType.String) region = reader.GetString();
                    break;
                case "resources":
                    if (reader.TokenType == JsonTokenType.StartArray)
                    {
                        resources = new List<string>();
                        while (reader.Read() && reader.TokenType != JsonTokenType.EndArray)
                        {
                            if (reader.TokenType == JsonTokenType.String)
                            {
                                var v = reader.GetString();
                                if (!string.IsNullOrEmpty(v)) resources.Add(v);
                            }
                            else
                            {
                                reader.Skip();
                            }
                        }
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
                Time = time ?? default,
                AWSAccount = account,
                AWSRegion = region,
                Resources = resources
            }
        };
        innerPayload = detailBytes;
        return true;
    }
}

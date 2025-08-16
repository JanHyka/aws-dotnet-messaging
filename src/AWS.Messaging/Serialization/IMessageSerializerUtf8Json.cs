// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System;
using System.Text.Json;

namespace AWS.Messaging.Serialization;

/// <summary>
/// Supports serialization and deserialization of domain-specific application messages.
/// This interface extends <see cref="IMessageSerializer"/> to provide a methods for allocation-free serialization/deserialization.
/// </summary>
public interface IMessageSerializerUtf8Json
{
    /// <summary>
    /// Serializes the .NET message object into a UTF-8 JSON string using a Utf8JsonWriter.
    /// </summary>
    /// <param name="writer">Utf8JsonWriter to write the serialized data.</param>
    /// <param name="value">The .NET object that will be serialized.</param>
    void SerializeToBuffer<T>(Utf8JsonWriter writer, T value);

    /// <summary>
    /// Deserializes a UTF-8 JSON payload into the specified .NET type without intermediate string allocation.
    /// </summary>
    /// <param name="utf8Json">The JSON payload as UTF-8 bytes.</param>
    /// <param name="deserializedType">The target .NET type.</param>
    /// <returns>The deserialized object.</returns>
    object Deserialize(ReadOnlySpan<byte> utf8Json, Type deserializedType);

    /// <summary>
    /// Deserializes from an existing Utf8JsonReader into the specified .NET type.
    /// </summary>
    /// <param name="reader">An existing Utf8JsonReader positioned at the start of the value.</param>
    /// <param name="deserializedType">The target .NET type.</param>
    /// <returns>The deserialized object.</returns>
    object Deserialize(ref Utf8JsonReader reader, Type deserializedType);

    /// <summary>
    /// Gets the MIME type of the content.
    /// </summary>
    string ContentType { get; }
}

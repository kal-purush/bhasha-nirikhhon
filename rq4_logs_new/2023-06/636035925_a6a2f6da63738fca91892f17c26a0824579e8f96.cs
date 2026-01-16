// Copyright © 2023 EPAM Systems, Inc. All Rights Reserved. All information contained herein is, and remains the
// property of EPAM Systems, Inc. and/or its suppliers and is protected by international intellectual
// property law. Dissemination of this information or reproduction of this material is strictly forbidden,
// unless prior written permission is obtained from EPAM Systems, Inc

using System.Runtime.Serialization;

namespace Cart.Business.Exceptions;

[Serializable]
public class EntityNotFoundException : Exception, ISerializable
{
    public EntityNotFoundException() : base() { }
    public EntityNotFoundException(string message) : base(message) { }
    public EntityNotFoundException(string message, Exception innerException) : base(message, innerException) { }
    protected EntityNotFoundException(SerializationInfo info, StreamingContext context) : base(info, context) { }
}
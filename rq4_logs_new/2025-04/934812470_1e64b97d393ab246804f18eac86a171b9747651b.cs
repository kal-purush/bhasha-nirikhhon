// Copyright (c) All contributors. All rights reserved. Licensed under the MIT license.

using System.Diagnostics.CodeAnalysis;

namespace Netsphere.Crypto;

public static class Alias
{// Identifier/PublicKey <-> Alias
    public const int MaxAliasLength = 32; //  <= RawPublicKeyLengthInBase64
    private static readonly Lock LockPublicKey = new();
    private static readonly NotThreadsafeHashtable<SignaturePublicKey, string> PublicKeyToAliasTable = new();
    private static readonly Utf16Hashtable<SignaturePublicKey> AliasToPublicKeyTable = new();

    private static readonly Lock LockIdentifier = new();
    private static readonly NotThreadsafeHashtable<Identifier, string> IdentifierToAliasTable = new();
    private static readonly Utf16Hashtable<Identifier> AliasToIdentifierTable = new();

    public static void AddAlias(SignaturePublicKey publicKey, string alias)
    {
        if (alias.Length > MaxAliasLength)
        {
            throw new ArgumentOutOfRangeException(nameof(alias), $"Alias length must be less than {MaxAliasLength}.");
        }

        using (LockPublicKey.EnterScope())
        {
            PublicKeyToAliasTable.Add(publicKey, alias);
            AliasToPublicKeyTable.Add(alias, publicKey);
        }
    }

    public static void ClearPublicKeyAlias()
    {
        using (LockPublicKey.EnterScope())
        {
            PublicKeyToAliasTable.Clear();
            AliasToPublicKeyTable.Clear();
        }
    }

    public static void AddAlias(Identifier identifier, string alias)
    {
        if (alias.Length > MaxAliasLength)
        {
            throw new ArgumentOutOfRangeException(nameof(alias), $"Alias length must be less than {MaxAliasLength}.");
        }

        using (LockIdentifier.EnterScope())
        {
            IdentifierToAliasTable.Add(identifier, alias);
            AliasToIdentifierTable.Add(alias, identifier);
        }
    }

    public static void ClearIdentifierAlias()
    {
        using (LockIdentifier.EnterScope())
        {
            IdentifierToAliasTable.Clear();
            AliasToIdentifierTable.Clear();
        }
    }

    public static bool TryGetAliasFromPublicKey(SignaturePublicKey publicKey, [MaybeNullWhen(false)] out string alias)
        => PublicKeyToAliasTable.TryGetValue(publicKey, out alias);

    public static bool TryGetPublicKeyFromAlias(ReadOnlySpan<char> alias, out SignaturePublicKey publicKey)
        => AliasToPublicKeyTable.TryGetValue(alias, out publicKey);

    public static bool TryGetAliasFromIdentifier(Identifier identifier, [MaybeNullWhen(false)] out string alias)
        => IdentifierToAliasTable.TryGetValue(identifier, out alias);

    public static bool TryGetIdentifierFromAlias(ReadOnlySpan<char> alias, out Identifier identifier)
        => AliasToIdentifierTable.TryGetValue(alias, out identifier);
}
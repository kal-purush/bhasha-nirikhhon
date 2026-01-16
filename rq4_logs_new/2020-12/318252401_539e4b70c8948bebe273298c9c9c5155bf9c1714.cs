using System;
using System.Collections.Generic;
using Uno;

namespace Chinook.Persistence
{
	/// <summary>
	/// Transactional Context for the Update callback.
	/// </summary>
	public class DataPersisterTransactionContext<T>
	{
		/// <summary>
		/// Application-specific correlation tag for the UPDATE OPERATION.
		/// </summary>
		/// <remarks>
		/// Best-effort on DataPersister to correlate values with updates.
		/// This data is not expected to be persisted/serialized and is runtime only.
		/// **IMPORTANT: ONCE COMMITTED, THIS CORRELATION TAG WILL BECOME THE NEW CORRELATION TAG**
		/// </remarks>
		public object TransactionCorrelationTag { get; }

		public DataReaderLoadResult<T> Read { get; }

		/// <summary>
		/// Create an instance from a load result (readContext)
		/// </summary>
		/// <param name="readContext"></param>
		/// <param name="transactionCorrelationTag">Correlation tag of the transaction, will replace the existing on commited</param>
		public DataPersisterTransactionContext(DataReaderLoadResult<T> readContext, object transactionCorrelationTag)
		{
			Read = readContext ?? throw new ArgumentNullException(nameof(readContext));
			TransactionCorrelationTag = transactionCorrelationTag;
		}

		/// <summary>
		/// If the entity needs to be saved back
		/// </summary>
		public bool IsCommitted { get; private set; } = false;

		/// <summary>
		/// If the entity is removed.
		/// </summary>
		public bool IsRemoved { get; private set; } = false;

		/// <summary>
		/// The committed value.
		/// </summary>
		/// <remarks>
		/// You should take care to avoid confusion with base.Value who is the initial value.
		/// </remarks>
		public T CommittedValue { get; private set; }

		/// <summary>
		/// This is a Option (Some/None) representation of the committed value
		/// </summary>
		public Option<T> CommittedOptionValue => IsCommitted && !IsRemoved ? Option.Some(CommittedValue) : Option.None<T>();

		/// <summary>
		/// Enlist the transaction to "committed" and set committed value as updated result.
		/// </summary>
		public void Commit(T committedValue)
		{
			var previousValue = Read.IsValuePresent ? Read.Value : default(T);
			var isEquals = ReferenceEquals(previousValue, committedValue)
						|| (Read.Provider.Comparer ?? EqualityComparer<T>.Default).Equals(previousValue, committedValue);

			CommittedValue = committedValue;
			IsCommitted = !isEquals; // Will commit only if the value changed from the previous.
			IsRemoved = false;
		}

		/// <summary>
		/// Tell the persister to Remove the value as updated result.
		/// </summary>
		/// <remarks>
		/// Won't commit if value already "removed".
		/// </remarks>
		public void RemoveAndCommit()
		{
			IsCommitted = Read.IsValuePresent; // Commit only if it was present in previous version
			IsRemoved = true;
			CommittedValue = default(T);
		}

		/// <summary>
		/// Enlist the transaction to "committed" and set committed value as updated result.
		/// </summary>
		/// <remarks>
		/// Using Option.None will do a Remove operation.
		/// </remarks>
		public void CommitOption(Option<T> committedOptionValue)
		{
			if (committedOptionValue?.MatchNone() == null)
			{
				throw new ArgumentNullException(nameof(committedOptionValue));
			}

			if (ReferenceEquals((T)Read.OptionValue, (T)committedOptionValue))
			{
				return; // Nothing to update
			}

			if (committedOptionValue.MatchSome(out var v))
			{
				Commit(v);
			}
			else if (committedOptionValue.MatchNone())
			{
				RemoveAndCommit();
			}
		}

		/// <summary>
		/// Reset the state of the context to uncommitted.
		/// </summary>
		public void Reset()
		{
			IsCommitted = false;
			IsRemoved = false;
			CommittedValue = default(T);
		}
	}

	public static class DataPersister2TransactionContextExtensions
	{
		public static T GetReadValueOrDefault<T>(this DataPersisterTransactionContext<T> persister, T defaultValue = default(T))
		{
			return persister.Read.GetValueOrDefault(defaultValue);
		}
	}
}
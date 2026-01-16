using System;
using System.Threading.Tasks;

namespace MyNihongo.Option.Extensions
{
	public static class OptionalElseExtensions
	{
		public static void OrElse(this OptionalElse @this, Action? action)
		{
			if (action == null)
				throw new ArgumentNullException(nameof(action));

			if (@this.ShouldExecute)
				action();
		}

		public static async ValueTask OrElseAsync(this OptionalElse @this, Func<Task>? actionAsync)
		{
			if (actionAsync == null)
				throw new ArgumentNullException(nameof(actionAsync));

			if (@this.ShouldExecute)
				await actionAsync()
					.ConfigureAwait(false);
		}

		public static async ValueTask OrElseAsync(this OptionalElse @this, Func<ValueTask>? actionAsync)
		{
			if (actionAsync == null)
				throw new ArgumentNullException(nameof(actionAsync));

			if (@this.ShouldExecute)
				await actionAsync()
					.ConfigureAwait(false);
		}

		public static async ValueTask OrElseAsync(this ValueTask<OptionalElse> @this, Action? action)
		{
			if (action == null)
				throw new ArgumentNullException(nameof(action));

			var optionalElse = await @this.ConfigureAwait(false);

			if (optionalElse.ShouldExecute)
				action();
		}

		public static async ValueTask OrElseAsync(this ValueTask<OptionalElse> @this, Func<Task>? actionAsync)
		{
			if (actionAsync == null)
				throw new ArgumentNullException(nameof(actionAsync));

			var optionalElse = await @this.ConfigureAwait(false);

			if (optionalElse.ShouldExecute)
				await actionAsync()
					.ConfigureAwait(false);
		}

		public static async ValueTask OrElseAsync(this ValueTask<OptionalElse> @this, Func<ValueTask>? actionAsync)
		{
			if (actionAsync == null)
				throw new ArgumentNullException(nameof(actionAsync));

			var optionalElse = await @this.ConfigureAwait(false);

			if (optionalElse.ShouldExecute)
				await actionAsync()
					.ConfigureAwait(false);
		}
	}
}
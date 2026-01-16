using Microsoft.AspNetCore.Mvc.ModelBinding;

namespace Circle.Web.Models.Utilities.Binding
{
	public class HashtagsModelBinder : IModelBinder
	{
		public Task BindModelAsync(ModelBindingContext bindingContext)
		{
			if (bindingContext == null)
			{
				throw new ArgumentNullException(nameof(bindingContext));
			}

			var result = bindingContext.ValueProvider.GetValue("Hashtags").Values[0]?.Split(",").ToList();

			bindingContext.Result = ModelBindingResult.Success(result);
			return Task.CompletedTask;
		}
	}
}
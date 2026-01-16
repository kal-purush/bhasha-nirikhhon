using System;

namespace DSharpPlus.SlashCommands.TestBot
{
	public class LocalizationProvider : ILocalizationProvider
	{
		public string GetLocalizedString(LocalizationContext localizationContext, Localization language, string key) =>
			localizationContext switch
			{
				LocalizationContext.SlashCommandName => "scn-" + language.GetLanguageCode() + key,
				LocalizationContext.SlashCommandDescription => "scd-" + language.GetLanguageCode() + key,
				LocalizationContext.SlashCommandOptionName => "scon-" + language.GetLanguageCode() + key,
				LocalizationContext.SlashCommandOptionDescription => "scod-" + language.GetLanguageCode() + key,
				LocalizationContext.ContextMenuName => "cm-" + language.GetLanguageCode() + key,
				_ => null
			};
	}
}
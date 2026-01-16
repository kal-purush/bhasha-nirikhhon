using FluentAssertions;

namespace Factory.Tests;

public class ATranslator
{
    [Test]
    public void returns_the_correct_translation_for_car()
    {
        var translator = new FrenchTranslator();
        var result = translator.Translate("car");
        result.Should().Be("voiture");
    }
}

public class ATranslatorApp
{
    [Test]
    public void when_run_with_a_french_translator_returns_its_translator_and_the_correct_translation_for_car()
    {
        var app = new TranslatorApp(new FrenchTranslator(), "car");
        var output = app.Run();
        output.Should().Contain("FrenchTranslator");
        output.Should().Contain("*-* voiture *-*");

    }
}
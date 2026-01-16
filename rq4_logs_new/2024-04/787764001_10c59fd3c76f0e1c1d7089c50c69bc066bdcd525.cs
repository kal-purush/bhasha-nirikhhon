using TechTitans.Enums;
using TechTitans.ViewModels;

namespace TechTitans.Views.Components.EndOfYearRecap;

public partial class ListenerPersonalityScreen : ContentView
{
	public EndOfYearRecapViewModel ViewModel { get; set; }
	public ListenerPersonalityScreen(EndOfYearRecapViewModel _viewModel)
	{
		InitializeComponent();
		ViewModel = _viewModel;
		switch (ViewModel.ListenerPersonality)
		{
			case ListenerPersonality.Vanilla:
				MainContent.BackgroundColor = new Color(200, 200, 100);
				MainLabel.Text = "You've been categorized as Vanilla, indicating that you've listened to very few songs. That's absolutely fine! Everyone has their own preferences when it comes to music, and there's beauty in simplicity. Whether you choose to explore more or stick to your familiar tunes, the most important thing is that music brings you joy in its own special way.";
				break;
			case ListenerPersonality.Casual:
                MainContent.BackgroundColor = new Color(100, 200, 100);
                MainLabel.Text = "You're a Casual listener, someone who enjoys music without delving too deep into it. While you may not have listened to a vast number of songs, your appreciation for the tunes you've encountered is undeniable. Keep enjoying music at your own pace, letting it add flavor to your moments without overwhelming them.";
                break;
			case ListenerPersonality.Melophile:
                MainContent.BackgroundColor = new Color(100, 100, 100);
                MainLabel.Text = "Congratulations! You've been classified as a Melophile, someone who has listened to a lot of songs. Your passion for music knows no bounds, and your love for exploring different melodies is truly commendable. Keep listening, keep discovering, and let the rhythm of life guide you.";
                break;
			case ListenerPersonality.Explorer:
                MainContent.BackgroundColor = new Color(50, 0, 100);
                MainLabel.Text = "Kudos to you! You're an Explorer in the realm of music, always eager to delve into new genres and sounds. Your adventurous spirit leads you to uncharted musical territories, expanding your horizons with each tune you encounter. Embrace the journey of discovery and let the melodies carry you to places you've never been.";
                break;
		}
	}
}
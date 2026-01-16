package Articles;

import java.util.concurrent.ExecutionException;

import Core.DataHolder;
import Core.Navigation;
import Database.ArticlesAPI;
import Database.Models.Article;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Button;
import javafx.scene.control.Label;
import javafx.scene.layout.VBox;
import javafx.scene.text.Text;
import javafx.scene.text.TextFlow;

public class ArticleDetailsPage {
    public static void RegisterWithNavigation() {
        // Labels
        Label titleLabel = new Label("Article Details");
        titleLabel.setStyle("-fx-font-size: 20px; -fx-font-weight: bold;");

        // Article Details
        Label idLabel = new Label();
        Label levelLabel = new Label();
        Label groupsLabel = new Label();
        Label sensitiveLabel = new Label();
        Label title = new Label();
        Label shortDescription = new Label();
        Label keywords = new Label();
        Text body = new Text();
        TextFlow bodyFlow = new TextFlow(body);
        Label links = new Label();
        Label nonSensitiveTitle = new Label();
        Label nonSensitiveDescription = new Label();

        // Back Button
        Button backButton = new Button("Back");
        backButton.setOnAction(e -> Navigation.navigateTo("ArticlesListPage"));

        // Layout
        VBox layout = new VBox(10);
        layout.setPadding(new Insets(20));
        layout.setAlignment(Pos.TOP_LEFT);
        layout.getChildren().addAll(
                titleLabel,
                idLabel,
                levelLabel,
                groupsLabel,
                sensitiveLabel,
                new Label("Title:"), title,
                new Label("Short Description:"), shortDescription,
                new Label("Keywords:"), keywords,
                new Label("Body:"), bodyFlow,
                new Label("Links:"), links,
                new Label("Non-Sensitive Title:"), nonSensitiveTitle,
                new Label("Non-Sensitive Description:"), nonSensitiveDescription,
                backButton
        );

        Scene scene = new Scene(layout, 600, 800);

        //System.console().printf("" + DataHolder.getInstance().getSelectedArticleId(), null);
        // Populate Data When Scene is Shown
        scene.windowProperty().addListener((obs, oldWindow, newWindow) -> {
            if (newWindow != null) {
            	long articleId = DataHolder.getInstance().getSelectedArticleId();
                Article article = null;
				try {
					article = ArticlesAPI.getArticleAsync(articleId, "").get();
				} catch (InterruptedException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (ExecutionException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
                if (article != null) {
                    idLabel.setText("ID: " + article.getId());
                    levelLabel.setText("Level: " + article.getLevel());
                    groupsLabel.setText("Groups: " + String.join(", ", article.getGroups()));
                    sensitiveLabel.setText("Sensitive: " + article.isSensitive());
                    title.setText(article.getTitle());
                    shortDescription.setText(article.getShortDescription());
                    keywords.setText(String.join(", ", article.getKeywords()));
                    body.setText(article.getBody());
                    links.setText(String.join(", ", article.getLinks()));
                    nonSensitiveTitle.setText(article.getNonSensitiveTitle());
                    nonSensitiveDescription.setText(article.getNonSensitiveDescription());
                } else {
                    // Handle article not found
                    title.setText("Article not found.");
                }
            }
        });

        Navigation.registerScene("ArticleDetailsPage", scene);
    }
}
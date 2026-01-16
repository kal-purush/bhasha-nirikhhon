import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;

public class Game {
    //Core Components
    protected static JFrame frame;
    protected JPanel panel;
    protected CardLayout cardLayout;
    protected String username;

    /**
     * Ensures GUI creation happens on the Event Dispatch Thread
     */
    public static void main(String[] args) {
        SwingUtilities.invokeLater(Game::new);
    }

    /**
     * Initializes the game application
     */
    protected Game() {
        initializeFrame();
        initializePanel();
        showMainMenu();
        frame.setVisible(true);
    }

    /**
     * Configures the application's frame or window
     */
    protected void initializeFrame() {
        frame = new JFrame("Queeze");
        frame.setIconImage(new ImageIcon("Assets/queeze logo.png").getImage());
        frame.setSize(1280, 960);
        frame.setMinimumSize(new Dimension(1280, 960));
        frame.setLayout(new BorderLayout());
        frame.setLocationRelativeTo(null);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    }

    /**
     * Initializes the main panel with CardLayout
     */
    protected void initializePanel() {
        cardLayout = new CardLayout();
        panel = new JPanel(cardLayout);
        frame.add(panel);
    }

    /**
     * Creates and displays the main menu screen
     */
    protected void showMainMenu() {
        GameUtils.GradientPanel startPanel = new GameUtils.GradientPanel(new GridBagLayout(), Color.decode("#0A0A"), Color.decode("#8F00FF"));

        GridBagConstraints gbcTop = GameUtils.createGridBagConstraints();
        gbcTop.insets = new Insets(10, 10, 30, 10);

        ImageIcon originalIcon = new ImageIcon("Assets//queeze logo.png");
        Image scaledImage = originalIcon.getImage().getScaledInstance(300, 250, Image.SCALE_SMOOTH);
        ImageIcon scaledIcon = new ImageIcon(scaledImage);
        JLabel imageLabel = new JLabel(scaledIcon);
        startPanel.add(imageLabel, gbcTop);

        GridBagConstraints gbcMiddle = GameUtils.createGridBagConstraints();
        gbcMiddle.insets = new Insets(10, 10, 30, 10);

        JButton startButton = GameUtils.createButton("Start Game", new Dimension(550, 100),
                new Font("Poppins", Font.BOLD, 30), new Color(0x0FFFFF), Color.BLACK,
                e -> showUsernameEntry());
        startPanel.add(startButton, gbcMiddle);

        JButton leaderboardButton = GameUtils.createButton("Leaderboard", new Dimension(550, 100),
                new Font("Poppins", Font.BOLD, 30), new Color(0x0FFFFF), Color.BLACK,
                e -> showDifficultySelection(true));
        startPanel.add(leaderboardButton, gbcMiddle);

        JButton exitButton = GameUtils.createButton("Exit", new Dimension(550, 100),
                new Font("Poppins", Font.BOLD, 30), new Color(0x0FFFFF), Color.BLACK,
                e -> System.exit(0));
        startPanel.add(exitButton, gbcMiddle);

        GridBagConstraints gbcBottom = GameUtils.createGridBagConstraints();
        gbcBottom.insets = new Insets(10, 10, -70, 10);

        JLabel creditsLabel = GameUtils.createLabel("© 2025, UP-FBS2-BSIT1-04 (Group 1)", 15, Color.LIGHT_GRAY);
        startPanel.add(creditsLabel, gbcBottom);

        panel.add(startPanel, "StartScreen");
    }

    /**
     * Shows difficulty selection screen accordingly
     *
     * @param isLeaderboard - true if viewing leaderboards, false if starting a new game
     */
    protected void showDifficultySelection(boolean isLeaderboard) {
        GameUtils.GradientPanel difficultyPanel = new GameUtils.GradientPanel(new BorderLayout(), Color.decode("#0A0A"), Color.decode("#8F00FF"));

        JPanel centerPanel = GameUtils.createPanel(new GridBagLayout(), false);

        GridBagConstraints gbc = GameUtils.createGridBagConstraints();

        JLabel modeLabel = GameUtils.createLabel("Select Difficulty", 35, Color.WHITE);
        centerPanel.add(modeLabel, gbc);

        GameUtils.createGameModeButtons(centerPanel, gbc, isLeaderboard, this);

        JPanel topPanel = GameUtils.createReturnPanel(isLeaderboard ? "StartScreen" : "UsernameEntryScreen", panel, cardLayout);

        difficultyPanel.add(topPanel, BorderLayout.NORTH);
        difficultyPanel.add(centerPanel, BorderLayout.CENTER);

        panel.add(difficultyPanel, isLeaderboard ? "LeaderboardSelection" : "GameSelection");
        cardLayout.show(panel, isLeaderboard ? "LeaderboardSelection" : "GameSelection");
    }

    /**
     * Shows username entry screen for user record
     */
    protected void showUsernameEntry() {
        GameUtils.GradientPanel userEntryPanel = new GameUtils.GradientPanel(new BorderLayout(), Color.decode("#0A0A"), Color.decode("#8F00FF"));

        JPanel encodePanel = GameUtils.createPanel(new GridBagLayout(), false);

        GridBagConstraints gbc = GameUtils.createGridBagConstraints();

        JLabel usernameLabel = GameUtils.createLabel("Please Enter Your Username:", 40, Color.WHITE);
        encodePanel.add(usernameLabel, gbc);

        JTextField usernameTextField = new JTextField(15);
        usernameTextField.setPreferredSize(new Dimension(usernameTextField.getPreferredSize().width, 60));
        usernameTextField.setFont(new Font("Poppins", Font.BOLD, 25));
        usernameTextField.setHorizontalAlignment(JTextField.CENTER);
        usernameTextField.setForeground(Color.BLACK);
        encodePanel.add(usernameTextField, gbc);

        JButton encodeButton = GameUtils.createButton("Enter", new Dimension(250, 80),
                new Font("Poppins", Font.BOLD, 30), new Color(0x0FFFFF), Color.BLACK,
                e -> {
                    username = usernameTextField.getText();
                    if (!username.isEmpty()) {
                        showDifficultySelection(false);
                    } else {
                        JOptionPane.showMessageDialog(frame, "Please enter a username.", "Error", JOptionPane.ERROR_MESSAGE);
                    }
                });
        gbc.fill = GridBagConstraints.NONE;
        gbc.anchor = GridBagConstraints.CENTER;
        encodePanel.add(encodeButton, gbc);

        userEntryPanel.add(GameUtils.createReturnPanel("StartScreen", panel, cardLayout), BorderLayout.NORTH);
        userEntryPanel.add(encodePanel, BorderLayout.CENTER);
        panel.add(userEntryPanel, "UsernameEntryScreen");
        cardLayout.show(panel, "UsernameEntryScreen");
    }

    /**
     * Handles game start request for specified difficulty
     * Checks for existing saves before starting
     */
    protected void showGameStart(String difficulty) {
        GameUtils.mainMenuLoadGameState(username, difficulty, this);
    }

    /**
     * Starts game with loaded save state
     */
    protected void startWithSaveState(String difficulty, GameUtils.GameState gameState) {
        Base gameMode = this.createGameMode(difficulty, username);
        gameMode.showGameplay(panel, cardLayout, gameState.getScore(), gameState.getLives(), gameState.getQuestionIndex());
    }

    /**
     * Starts a new game with the initial lives depending on the difficulty
     */
    protected void startNewRound(String difficulty) {
        Base gameMode = this.createGameMode(difficulty, username);
        int initialLives = gameMode.initialLives();
        gameMode.showGameplay(panel, cardLayout, 0, initialLives, 0);
    }

    /**
     * Factory method for creating game mode instances depending on the difficulty
     */
    protected Base createGameMode(String difficulty, String username) {
        switch (difficulty) {
            case "Easy":
                return new GameModes.EasyMode(username);
            case "Normal":
                return new GameModes.NormalMode(username);
            case "Hard":
                return new GameModes.HardMode(username);
            default:
                throw new IllegalArgumentException("Invalid difficulty: " + difficulty);
        }
    }

    /**
     * Displays leaderboard for specified difficulty
     * Adds crown symbol (♛) for either the top scorer or those with the highest possible scores (15)
     */
    protected void showLeaderboard(String difficulty) {
        GameUtils.GradientPanel leaderboardPanel = new GameUtils.GradientPanel(new BorderLayout(), Color.decode("#0A0A"), Color.decode("#8F00FF"));

        JPanel centerPanel = GameUtils.createPanel(new GridBagLayout(), false);

        GridBagConstraints gbc = GameUtils.createGridBagConstraints();

        JLabel titleLabel = GameUtils.createLabel("Leaderboard " + "(" + difficulty + ")", 45, Color.WHITE);
        centerPanel.add(titleLabel, gbc);

        JPanel scoresPanel = GameUtils.createPanel(new GridLayout(0, 1, 10, 10), false);

        List<GameUtils.PlayerScore> scores = GameUtils.getPlayerScore(difficulty);
        if (scores.isEmpty()) {
            JLabel noScoresLabel = GameUtils.createLabel("No scores recorded yet", 25, Color.WHITE);
            scoresPanel.add(noScoresLabel);
        } else {
            for (int i = 0; i < scores.size(); i++) {
                GameUtils.PlayerScore playerScore = scores.get(i);
                String scoreText = playerScore.toString();
                if (i == 0 || playerScore.getScore() == 15) {
                    scoreText = "♛ " + scoreText;
                }
                JLabel scoreLabel = GameUtils.createLabel(scoreText, 25, Color.WHITE);
                scoresPanel.add(scoreLabel);
            }
        }

        centerPanel.add(scoresPanel, gbc);
        leaderboardPanel.add(centerPanel, BorderLayout.CENTER);

        JPanel topPanel = GameUtils.createReturnPanel("LeaderboardSelection", panel, cardLayout);
        leaderboardPanel.add(topPanel, BorderLayout.NORTH);

        JScrollPane scrollPane = new JScrollPane(leaderboardPanel);
        scrollPane.setBorder(BorderFactory.createEmptyBorder());
        scrollPane.getVerticalScrollBar().setUnitIncrement(16);
        scrollPane.setVerticalScrollBarPolicy(ScrollPaneConstants.VERTICAL_SCROLLBAR_ALWAYS);
        scrollPane.setHorizontalScrollBarPolicy(JScrollPane.HORIZONTAL_SCROLLBAR_NEVER);

        panel.add(scrollPane, "Leaderboard");
        cardLayout.show(panel, "Leaderboard");
    }


    public static abstract class Base implements ActionListener {
        //Core components
        protected JPanel panel;
        protected CardLayout cardLayout;

        protected String username;
        protected abstract int initialLives();
        protected int score = 0;
        protected int lives;
        protected int questionIndex = 0;
        protected int answerIndex = 0;

        protected String[] questions;
        protected String[][] options;
        protected String[] correctAnswers;
        protected JButton pauseButton;
        protected JLabel scoreLabel;
        protected JLabel livesLabel;
        protected JLabel questionLabel;
        protected JButton[] optionButtons = new JButton[4];
        protected JButton toggleMusicButton;

        /**
         * Gets difficulty identifier from game mode classes
         * @return difficulty
         */
        protected abstract String getDifficulty();

        /**
         * Initializes game with player username depending on the difficulty chosen
         * @param username - Player's username
         */
        public Base(String username) {
            this.username = username;
            this.lives = initialLives();
        }

        /**
         * Initializes and displays the main gameplay screen
         *
         * @param panel - The frame's main panel
         * @param cardLayout - The frame's CardLayout
         * @param score - Starting score
         * @param lives - Starting lives
         * @param questionIndex - Starting question position
         */
        public void showGameplay(JPanel panel, CardLayout cardLayout, int score, int lives, int questionIndex) {
            this.panel = panel;
            this.cardLayout = cardLayout;
            this.score = score;
            this.lives = lives;
            this.questionIndex = questionIndex;

            GameUtils.GradientPanel gamePanel = new GameUtils.GradientPanel(new BorderLayout(), Color.decode("#F0F0F0"), Color.decode("#D8D8FF"));

            JPanel questionPanel = GameUtils.createPanel(new GridBagLayout(), false);

            GridBagConstraints gbc = GameUtils.createGridBagConstraints();

            questionLabel = GameUtils.createLabel("",  25, Color.BLACK);
            gbc.weighty = 1;
            gbc.insets = new Insets(10, 10, 10, 10);
            questionPanel.add(questionLabel, gbc);

            gbc.insets = new Insets(-100, 40, 0, 40);
            gbc.fill = GridBagConstraints.NONE;

            for (int i = 0; i < 4; i++) {
                optionButtons[i] = GameUtils.createButton("", new Dimension(400, 200), new Font("Roboto",
                        Font.BOLD, 20), new Color(0x004EA1), Color.WHITE, this);
                gbc.gridx = i % 2;
                gbc.gridy = i / 2 + 1;
                gbc.gridwidth = 1;
                questionPanel.add(optionButtons[i], gbc);
            }

            JPanel pausePanel = GameUtils.createPanel(new GridBagLayout(), false);
            pausePanel.setBorder(BorderFactory.createEmptyBorder(20, 20, 20, 20));

            pauseButton = GameUtils.createButton("Pause", new Dimension(100, 30),
                    new Font("Roboto Mono", Font.BOLD, 20), new Color(0x004EA1), Color.WHITE, e -> {
                        showPauseMenu();
                    });

            pausePanel.add(pauseButton, new GridBagConstraints());

            JPanel infoPanel = GameUtils.createPanel(new GridBagLayout(), false);
            infoPanel.setBorder(BorderFactory.createEmptyBorder(20, 20, 20, 20));

            JPanel labelsPanel = GameUtils.createPanel(new FlowLayout(FlowLayout.CENTER, 20, 0), false);
            scoreLabel = GameUtils.createLabel("Score: " + score + "/15", 20, Color.BLACK);
            livesLabel = GameUtils.createLabel("Lives: " + lives, 20, Color.BLACK);
            labelsPanel.add(scoreLabel);
            labelsPanel.add(livesLabel);

            infoPanel.add(labelsPanel, new GridBagConstraints());

            JPanel topPanel = GameUtils.createPanel(new BorderLayout(), false);
            topPanel.add(pausePanel, BorderLayout.NORTH);
            topPanel.add(infoPanel, BorderLayout.CENTER);

            gamePanel.add(topPanel, BorderLayout.NORTH);
            gamePanel.add(questionPanel, BorderLayout.CENTER);

            panel.add(gamePanel, "GameScreen");
            cardLayout.show(panel, "GameScreen");
            loadNextQuestion();
            toggleBackgroundMusic();
        }

        /**
         * Displays pause overlay menu with game control options
         */
        protected void showPauseMenu() {
            pauseButton.setEnabled(false);
            for (JButton button : optionButtons) {
                button.setEnabled(false);
            }

            JPanel overlayPanel = GameUtils.showPauseOverlay(frame, panel);

            JPanel pauseMenuPanel = GameUtils.createPanel(new GridBagLayout(), true);
            pauseMenuPanel.setOpaque(false);

            JButton resumeButton = GameUtils.createButton("Resume", new Dimension(300, 60),
                    new Font("Roboto Mono", Font.BOLD, 20), new Color(0x004EA1), Color.WHITE,
                    e -> {
                        GameUtils.removePauseOverlay();
                        GameUtils.resumeGame(pauseButton, optionButtons);
                    });

            toggleMusicButton = GameUtils.createButton("Stop Music", new Dimension(300, 60),
                    new Font("Roboto Mono", Font.BOLD, 20), new Color(0x004EA1), Color.WHITE,
                    e -> toggleBackgroundMusic());

            JButton saveMenuButton = GameUtils.createButton("Save Menu", new Dimension(300, 60),
                    new Font("Roboto Mono", Font.BOLD, 20), new Color(0x004EA1), Color.WHITE,
                    e -> {
                        showSaveMenu();
                    });

            JButton returnButton = GameUtils.createButton("Main Menu", new Dimension(300, 60),
                    new Font("Roboto Mono", Font.BOLD, 20), new Color(0x004EA1), Color.WHITE,
                    e -> {
                        GameUtils.removePauseOverlay();
                        returnToMenu();
                    });

            JButton exitButton = GameUtils.createButton("Exit Game", new Dimension(300, 60),
                    new Font("Roboto Mono", Font.BOLD, 20), new Color(0x004EA1), Color.WHITE,
                    e -> {
                        GameUtils.exitWarning();
                    });

            GridBagConstraints gbc = GameUtils.createGridBagConstraints();
            gbc.insets = new Insets(5, 10, 5, 10);
            pauseMenuPanel.add(resumeButton, gbc);
            pauseMenuPanel.add(toggleMusicButton, gbc);
            pauseMenuPanel.add(saveMenuButton, gbc);
            pauseMenuPanel.add(returnButton, gbc);
            pauseMenuPanel.add(exitButton, gbc);

            overlayPanel.add(pauseMenuPanel);
            frame.revalidate();
            frame.repaint();
        }

        /**
         * Displays save state management submenu
         */
        protected void showSaveMenu () {
            JPanel overlayPanel = GameUtils.showPauseOverlay(frame, panel);

            JPanel saveMenuPanel = GameUtils.createPanel(new GridBagLayout(), true);
            saveMenuPanel.setOpaque(false);

            JButton backButton = GameUtils.createButton("Back", new Dimension(300, 60),
                    new Font("Roboto Mono", Font.BOLD, 20), new Color(0x004EA1), Color.WHITE,
                    e -> {
                        showPauseMenu();
                    });

            JButton saveButton = GameUtils.createButton("Save Game", new Dimension(300, 60),
                    new Font("Roboto Mono", Font.BOLD, 20), new Color(0x004EA1), Color.WHITE,
                    e -> {
                        GameUtils.saveGameState(username, score, lives, questionIndex, getDifficulty(), panel, cardLayout);
                    });

            JButton deleteButton = GameUtils.createButton("Delete Save", new Dimension(300, 60),
                    new Font("Roboto Mono", Font.BOLD, 20), new Color(0x004EA1), Color.WHITE,
                    e -> {
                        GameUtils.deleteGameState(username, getDifficulty());
                    });

            JButton loadButton = GameUtils.createButton("Load Save", new Dimension(300, 60),
                    new Font("Roboto Mono", Font.BOLD, 20), new Color(0x004EA1), Color.WHITE,
                    e -> {
                        GameUtils.gameplayLoadGameState(username, getDifficulty(), this);
                    });

            GridBagConstraints gbc = GameUtils.createGridBagConstraints();
            gbc.insets = new Insets(5, 10, 5, 10);
            saveMenuPanel.add(backButton, gbc);
            saveMenuPanel.add(saveButton, gbc);
            saveMenuPanel.add(deleteButton, gbc);
            saveMenuPanel.add(loadButton, gbc);

            overlayPanel.add(saveMenuPanel);
            frame.revalidate();
            frame.repaint();
        }


        //Gameplay Methods//

        /**
         * Toggles background music playback and updates button text
         */
        protected void toggleBackgroundMusic() {
            GameUtils.toggleBackgroundMusic();
            if (toggleMusicButton != null) {
                if (GameUtils.backgroundMusic != null && GameUtils.backgroundMusic.isRunning()) {
                    toggleMusicButton.setText("Stop Music");
                } else {
                    toggleMusicButton.setText("Play Music");
                }
            }
        }

        /**
         * Handles answer selection logic
         *
         * @param e - ActionEvent from clicking option buttons
         */
        @Override
        public void actionPerformed(ActionEvent e) {
            JButton clickedButton = (JButton) e.getSource();
            String selectedAnswer = clickedButton.getText();

            if (selectedAnswer.equals(correctAnswers[answerIndex])) {
                score++;
                scoreLabel.setText("Score: " + score + "/15");
                clickedButton.setBackground(new Color(0x0CCF6D));
                clickedButton.setForeground(Color.BLACK);
                JOptionPane.showMessageDialog(null, "Correct!", "Evaluation", JOptionPane.INFORMATION_MESSAGE);
            }
            else {
                lives--;
                livesLabel.setText("Lives: " + lives);
                clickedButton.setBackground(new Color(0xFA4848));
                clickedButton.setForeground(Color.BLACK);
                JOptionPane.showMessageDialog(null, "Wrong!", "Evaluation", JOptionPane.INFORMATION_MESSAGE);
            }
            questionIndex++;
            answerIndex++;
            if (questionIndex < questions.length && lives > 0) {
                loadNextQuestion();
            } else if (questionIndex == questions.length && lives > 0) {
                showPostGameScreen(true, getDifficulty());
            } else {
                showPostGameScreen(false, getDifficulty());
            }
        }

        /**
         * Loads next question or ends game if none remain
         * Uses HTML styling for question text for proper display
         */
        protected void loadNextQuestion() {
            for (JButton button : optionButtons) {
                button.setBackground(new Color(0x004EA1));
                button.setForeground(Color.WHITE);
            }

            if (questionIndex < questions.length && lives > 0) {
                questionLabel.setText("<html><div style='width: 600px; text-align: center;'>" + questions[questionIndex] + "</div></html>");
                for (int i = 0; i < 4; i++) {
                    optionButtons[i].setText(options[questionIndex][i]);
                }
            }
        }


        //Post-Game Methods//

        /**
         * Displays post-game screen accordingly
         *
         * @param isWin - true if player completed all questions, false if lost all lives
         * @param difficulty - current game difficulty
         */
        protected void showPostGameScreen(boolean isWin, String difficulty) {
            GameUtils.stopBackgroundMusic();
            GameUtils.addScoreToLeaderboard(difficulty, username, score);

            GameUtils.GradientPanel postGamePanel = new GameUtils.GradientPanel(new GridBagLayout(), Color.decode("#0A0A"), Color.decode("#8F00FF"));

            GridBagConstraints gbc = GameUtils.createGridBagConstraints();

            JLabel titleLabel = GameUtils.createLabel(isWin ? "Congratulations!" : "Game Over!", 25, Color.WHITE);
            JLabel finalScoreLabel = GameUtils.createLabel("Final Score: " + score, 25, Color.WHITE);

            JButton restartButton = GameUtils.createButton("Restart Game", new Dimension(550, 100),
                    new Font("Poppins", Font.BOLD, 25), new Color(0x0FFFFF), Color.BLACK, e -> restartGame());

            JButton difficultyButton = GameUtils.createButton("Choose Another Difficulty", new Dimension(550, 100),
                    new Font("Poppins", Font.BOLD, 25), new Color(0x0FFFFF), Color.BLACK, e -> chooseDifficulty());

            JButton returnButton = GameUtils.createButton("Return to Main Menu", new Dimension(550, 100),
                    new Font("Poppins", Font.BOLD, 25), new Color(0x0FFFFF), Color.BLACK, e -> returnToMenu());

            postGamePanel.add(titleLabel, gbc);
            postGamePanel.add(finalScoreLabel, gbc);
            postGamePanel.add(restartButton, gbc);
            postGamePanel.add(difficultyButton, gbc);
            postGamePanel.add(returnButton, gbc);

            panel.add(postGamePanel, isWin ? "You Win" : "GameOver");
            cardLayout.show(panel, isWin ? "You Win" : "GameOver");
        }

        /**
         * Restarts game with fresh state at current difficulty
         */
        protected void restartGame() {
            GameUtils.stopBackgroundMusic();
            score = 0;
            questionIndex = 0;
            answerIndex = 0;
            lives = initialLives();
            scoreLabel.setText("Score: " + score);
            livesLabel.setText("Lives: " + lives);
            loadNextQuestion();
            GameUtils.playBackgroundMusic("assets//Itty Bitty.wav");
            cardLayout.show(panel, "GameScreen");
        }

        /**
         * Returns to difficulty selection screen
         */
        protected void chooseDifficulty() {
            GameUtils.stopBackgroundMusic();
            score = 0;
            questionIndex = 0;
            answerIndex = 0;
            cardLayout.show(panel, "GameSelection");
        }

        /**
         * Returns to main menu
         */
        protected void returnToMenu() {
            GameUtils.stopBackgroundMusic();
            score = 0;
            questionIndex = 0;
            answerIndex = 0;
            cardLayout.show(panel, "StartScreen");
        }
    }
}
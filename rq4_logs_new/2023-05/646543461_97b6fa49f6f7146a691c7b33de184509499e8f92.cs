using UnityEngine;

public class TriviaCategory
{
    public string categoryName;
    public Question[] questions;

    public TriviaCategory(string categoryName, Question[] questions)
    {
        this.categoryName = categoryName;
        this.questions = questions;
    }
}

public struct Question
{
    public string question;
    public string answer;

    public Question(string question, string answer)
    {
        this.question = question;
        this.answer = answer;
    }
}

public class TriviaControl : MonoBehaviour
{
    public int questionIndex = -1;

    [SerializeField] LogControl log;
    [SerializeField] GameData data;

    public Question GetRandomQuestion(int currentIndex)
    {
        if (currentIndex >= 0 && currentIndex < categories.Length)
        {
            TriviaCategory category = categories[currentIndex];
            if (category.questions.Length > 0)
            {
                questionIndex = Random.Range(0, category.questions.Length);
                return category.questions[questionIndex];
            }
            else
            {
                Debug.LogWarning("No questions available for the selected category.");
            }
        }
        else
        {
            Debug.LogError("Invalid category index!");
        }
        
        return default(Question);
    }

    public void CheckQuestion(int currentIndex, int questionIndex, string answer)
    {
    if (currentIndex >= 0 && currentIndex < categories.Length)
    {
        TriviaCategory category = categories[currentIndex];
        if (questionIndex >= 0 && questionIndex < category.questions.Length)
        {
            Question question = category.questions[questionIndex];
            if (question.answer.ToLower().Contains(answer.ToLower()))
            {
                Debug.Log("Correct!");
                log.AddLog("Correct!");
                data.categories[data.currentIndex].isAnswered = true;
            }
            else
            {
                Debug.Log("Incorrect!");
                log.AddLog("Incorrect!");
            }
        }
        else
        {
            Debug.LogError("Invalid question index!");
        }
    }
    else
    {
        Debug.LogError("Invalid category index!");
    }
}

    private TriviaCategory[] categories = new TriviaCategory[]
    {
        new TriviaCategory("Geography", new Question[]
        {
        new Question("What is the capital of France?", "Paris"),
        new Question("Which ocean is the largest?", "Pacific Ocean"),
        new Question("What is the highest mountain in the world?", "Mount Everest"),
        new Question("Which country is known as the 'Land of the Rising Sun'?", "Japan"),
        new Question("What is the largest desert in the world?", "Sahara Desert"),
        new Question("Which river is the longest in the world?", "Nile"),
        new Question("In which country is the Great Barrier Reef located?", "Australia"),
        new Question("Which city is located at the junction of the Danube and Sava rivers?", "Belgrade"),
        new Question("Which country is famous for its tulips and windmills?", "Netherlands"),
        new Question("What is the largest country by land area?", "Russia")
        }),

        new TriviaCategory("Entertainment", new Question[]
        {
            new Question("Who played the character of Tony Stark in the Marvel Cinematic Universe?", "Robert Downey Jr."),
            new Question("Which actress won the Best Actress Oscar for her role in 'La La Land'?", "Emma Stone"),
            new Question("Which band is known for the hit song 'Hotel California'?", "Eagles"),
            new Question("Which actor portrayed the character of Captain Jack Sparrow in the 'Pirates of the Caribbean' series?", "Johnny Depp"),
            new Question("Who is the author of the 'Harry Potter' book series?", "J.K. Rowling"),
            new Question("Which movie won the Best Picture Oscar in 2020?", "Parasite"),
            new Question("Which singer released the album 'Reputation' in 2017?", "Taylor Swift"),
            new Question("Which TV series is set in the fictional world of Westeros?", "Game of Thrones"),
            new Question("Who directed the movie 'Inception'?", "Christopher Nolan"),
            new Question("Which actor played the role of Wolverine in the 'X-Men' film series?", "Hugh Jackman")
        }),

        new TriviaCategory("History", new Question[]
        {
            new Question("In what year did World War II end?", "1945"),
            new Question("Who was the first President of the United States?", "George Washington"),
            new Question("Which city hosted the ancient Olympic Games?", "Olympia"),
            new Question("Who was the leader of the Soviet Union during World War II?", "Joseph Stalin"),
            new Question("Which year marked the beginning of the French Revolution?", "1789"),
            new Question("Which country was Nelson Mandela the President of?", "South Africa"),
            new Question("Which American President is credited with the Emancipation Proclamation?", "Abraham Lincoln"),
            new Question("Which explorer is known for circumnavigating the globe?", "Ferdinand Magellan"),
            new Question("Which country was the birthplace of the Renaissance?", "Italy"),
            new Question("Who was the first woman to win a Nobel Prize?", "Marie Curie")
        }),

        new TriviaCategory("Roll Again", new Question[]{}),

        new TriviaCategory("Arts & Literature", new Question[]
        {
            new Question("Who painted the 'Mona Lisa'?", "Leonardo da Vinci"),
            new Question("Who wrote the play 'Romeo and Juliet'?", "William Shakespeare"),
            new Question("Which author wrote the 'Lord of the Rings' trilogy?", "J.R.R. Tolkien"),
            new Question("Who is the author of the novel 'To Kill a Mockingbird'?", "Harper Lee"),
            new Question("Who is the playwright of 'Hamlet'?", "William Shakespeare"),
            new Question("Which artist is known for his paintings of Campbell's Soup cans?", "Andy Warhol"),
            new Question("Who is the author of the 'Harry Potter' book series?", "J.K. Rowling"),
            new Question("Which novel tells the story of a man named Dorian Gray?", "The Picture of Dorian Gray"),
            new Question("Who wrote the poem 'The Raven'?", "Edgar Allan Poe"),
            new Question("Which artist is known for his 'Starry Night' painting?", "Vincent van Gogh")
        }),

        new TriviaCategory("Science & Nature", new Question[]
        {
            new Question("What is the smallest unit of matter?", "Atom"),
            new Question("What is the largest organ in the human body?", "Skin"),
            new Question("Which planet is known as the 'Red Planet'?", "Mars"),
            new Question("What is the chemical symbol for the element Oxygen?", "O"),
            new Question("What is the process by which plants convert sunlight into energy called?", "Photosynthesis"),
            new Question("Which scientist formulated the theory of general relativity?", "Albert Einstein"),
            new Question("What is the study of living organisms called?", "Biology"),
            new Question("Which gas makes up the majority of Earth's atmosphere?", "Nitrogen"),
            new Question("What is the unit of measurement for electric current?", "Ampere"),
            new Question("Which animal is known for its ability to change its color to match its surroundings?", "Chameleon")
        }),

        new TriviaCategory("Sports & Leisure", new Question[]
        {
            new Question("Which sport is often called 'the gentleman's game'?", "Cricket"),
            new Question("In which country did the sport of soccer originate?", "England"),
            new Question("Which country has won the most FIFA World Cup titles?", "Brazil"),
            new Question("What is the highest possible score in a single frame of bowling?", "300"),
            new Question("Which tennis player has won the most Grand Slam singles titles?", "Roger Federer"),
            new Question("Which sport uses a shuttlecock?", "Badminton"),
            new Question("Who is the all-time leading scorer in NBA history?", "Kareem Abdul-Jabbar"),
            new Question("Which sport is played at Augusta National Golf Club?", "Golf"),
            new Question("In which city are the Summer Olympic Games scheduled to be held in 2028?", "Los Angeles"),
            new Question("Which athlete is known as 'The Greatest' and is a boxing legend?", "Muhammad Ali")
        })
    };
}
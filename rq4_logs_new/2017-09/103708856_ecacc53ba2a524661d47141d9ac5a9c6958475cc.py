from media import Movie
import fresh_tomatoes


matrix_reloaded = Movie("Matrix Reloaded",
                       ("Neo and the rebel leaders estimate that they"
                        "have 72 hours until 250,000 probes discover"
                        "Zion and destroy it and its inhabitants."
                        "During this, Neo must decide how he can"
                        "save Trinity from a dark fate in his dreams."),
                        "https://upload.wikimedia.org/wikipedia/en/b/ba/Poster_-_The_Matrix_Reloaded.jpg",  # noqa
                        "https://www.youtube.com/watch?v=HVrGMnk5E_M")
print("New object created: " + matrix_reloaded.title)

good_will_hunting = Movie("Good Will Hunting",
                         ("Will Hunting (Damon) is a headstrong,"
                          "working-class genius. After one too many"
                          "run-ins with the law, Will's last chance"
                          "is a psychology professor (Williams)."
                          "Experience this powerful and"
                          "unforgettable movie"),
                          "https://upload.wikimedia.org/wikipedia/en/b/b8/Good_Will_Hunting_theatrical_poster.jpg",  # noqa
                          "https://www.youtube.com/watch?v=QSMvyuEeIyw")
print("New object created: " + good_will_hunting.title)

collateral = Movie("Collateral",
                  ("A cab driver realizes his current fare is a hit man that"
                   "has been having him drive around from mark to mark until" 
                   "the last witness to a crime is dead. When the cabbie" 
                   "finally figures out the truth, he must prevent the assassin" 
                   "from wiping out his last witness without becoming the next" 
                   "in the professional killer's line of casualties."),
                   "https://upload.wikimedia.org/wikipedia/en/4/45/Collateral_%28Movie%29.jpg",  # noqa
                   "https://www.youtube.com/watch?v=9BDx6ZPHV4w")
print("New object created: " + collateral.title)

movie_list = [matrix_reloaded,
            good_will_hunting,
            collateral]

fresh_tomatoes.open_movies_page(movie_list)
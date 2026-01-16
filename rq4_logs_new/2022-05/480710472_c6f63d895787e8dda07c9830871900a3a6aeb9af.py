import pickle


def main():
    rating = normalizeRating()
    total_ratings = normalizeTotalRating()
    score = {}


    for i in rating.keys():
        ratingValue = rating[i]
        total_ratingsValue = total_ratings[i]

        if total_ratingsValue == 0 or ratingValue == 0:
            scoreValue = 0
        else:
            scoreValue = 2 / (1 / ratingValue + 1 / total_ratingsValue)

        score[i] = scoreValue

    return score

def normalizeRating ():
    # get the dictionary
    rating = pickle.load(open('rating.pkl', 'rb'))

    for i in rating.keys():
        value = rating[i]
        value = value * 5
        rating[i] = value

    return rating


def normalizeTotalRating ():
    # get the dictionary
    total_ratings = pickle.load(open('total_ratings.pkl', 'rb'))
    scoreTotalRating = total_ratings.values()

    sc = sorted(scoreTotalRating)
    maxValue = sc[len(sc) - 1]
    minValue = sc[0]

    for i in total_ratings.keys():
        value = total_ratings[i]
        value = (value - minValue) / (maxValue - minValue) * 100
        total_ratings[i] = value

    return total_ratings

if __name__ == '__main__':
    score = main()
    with open('score.pkl', 'wb') as f1:
        pickle.dump(score, f1)
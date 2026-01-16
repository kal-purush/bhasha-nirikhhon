'''
* Recommender 
* jaccard similarity and then grab the highest ratings from the most similar users as long as those have never been rated by the target user.
'''

#read json file
import json

#importing app so global variables can be used
import app

#user that we are matching
#matchUser = ["/u/1138361/iheartmwpp", "/u/8545331/Professor-Flourish-and-Blotts", "/u/4286546/Missbexiee", "/u/1697963/lydiamaartin", "/u/609412/Crystallic-Rain"]
def recommender(matchUser):          

    #Find similar users
    jaccardDict = {}

    for key in app.userFavs:
        cInter = 0
        cUnion = len(matchUser)
        for author in app.userFavs[key]:
            if author in matchUser:
                cInter+=1
            else:
                cUnion+=1
                
        jaccardDict[key] = cInter/cUnion

    #Sorting Jaccard Dictionary    
    sortedJaccard = sorted(jaccardDict.items(), key=lambda kv: kv[1], reverse=True)
        
    authorsToLookAt = []
    authorStoryScore = {}

    #top twenty most similar
    for i in range(20):
        favList = app.userFavs[sortedJaccard[i][0]]
        for elem in favList:
            if elem not in matchUser:
                if elem in authorStoryScore:
                    #adds the similarity score to the previous score that way authors that show up multiple times have their weight increased.
                    #Not the best way to add but yolo
                    newSim = authorStoryScore[elem][0] + sortedJaccard[i][1]
                    authorStoryScore[elem] = (newSim, "")
                else:
                    authorsToLookAt.append(elem)
                    authorStoryScore[elem] = (sortedJaccard[i][1], "")        #saves the similarity score from the user and leaves the storylink blank for now.

    for elem in set(authorsToLookAt):
        if elem in app.topStories:
            simScore = authorStoryScore[elem][0]
            authorStoryScore[elem] = (simScore, app.topStories[elem][0])

    returnStr = ""
    returnStr += "Printing Author similarityScore story link.\n"
    for key in authorStoryScore:
        returnStr += str(key) + " " +  str(authorStoryScore[key][0]) + " " + str(authorStoryScore[key][1]) + "\n"
    return returnStr


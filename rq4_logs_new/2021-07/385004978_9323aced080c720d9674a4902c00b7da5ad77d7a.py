def generateHTML():

    htmlFile = open("/Users/KennethXu/Sites/index.html", 'w')
    configFile=open("/Users/KennethXu/test/config.txt", 'r')
    text=configFile.readline()
    text = configFile.readline()


    htmlFile.write("\n")
    htmlFile.write("<html>\n")
    htmlFile.write("<body>\n")
    htmlFile.write("<h1>Test Web </h1>\n")
    htmlFile.write("<p>Tweet doo "+text+ "da joobe2 </p>\n" )
    htmlFile.write("</body>\n")
    htmlFile.write("</html>\n")
    htmlFile.close()


generateHTML()
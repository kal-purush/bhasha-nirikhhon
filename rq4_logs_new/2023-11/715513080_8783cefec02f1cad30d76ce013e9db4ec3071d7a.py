import sys
import os

def main(argv):
    def download(video):
        
        # convert into Youtube obj to get features
        yt = YouTube(video)
        name = yt.title
        title = name
        name = f"\033[1;35;40m {name} \u001b[0m"

        # determine whether the user wants to change the title of the mp3 audio
        decision = input(f"the current title name is {name}. \nDo you wish to change it? Enter \033[1;31;40m Y \u001b[0m to do so. Otherwise enter any key to continue: ")
        
        while decision.capitalize().strip() == "Y":
            title = input("Enter the new title for the mp3: ")

            name = f"\033[1;35;40m {title} \u001b[0m"
            yt.title = title

            decision = input(f"\nThe current title is {name}.\nDo you wish to change it? Enter \033[1;31;40m Y \u001b[0m to do so. Otherwise enter any key to continue: ")


        print('\n')
        # ask if the user wants to download the audio for the given video
        decision = input(f"Do you want to download the audio for this video? -> {name} \nEnter \033[1;31;40m Y \u001b[0m Otherwise enter any key to continue: ")

        if decision.capitalize().strip() != "Y":    
            print("\n")
            return

        # convert to an audio file  
        audio = yt.streams.filter(only_audio=True).first()
        directory = ""
        decision = "Y"

        while decision.capitalize().strip() == "Y":
            print('\n')
            print("Give a folder/directory to place the mp3 file or leave blank to leave it in current folder/directory")
            directory = str(input(" >> "))

            if len(directory) == 0:
                directory = "."

            print(f"\nThe given directory for the song {name} is \033[1;35;40m '{directory}' \u001b[0m")
            decision = input("Do you wish to change it?\nEnter \033[1;31;40m Y \u001b[0m to do so. Otherwise enter any key to continue: ")


        # download the audio
        download = audio.download(output_path=directory)

        # save the audio
        base, ext = os.path.splitext(download)
        audio_download = base + ".mp3"
        os.rename(download, audio_download)

        print(f"{name} has been downloaded\n")

            

    try:
        from pytube import YouTube
    except ImportError:
        print("Couldn't import |YouTube| from |pytube|")
        print('The needed command for Python3 would be "pip3 install pytube"')
        print(
            "\n***NOTE*** \n\tIt may not work if there are issues with your pip3 OR other...\n"
        )
        return

    
    for x in argv:
        download(x)

    output = "a"
    if len(argv) > 0:
        output = "another"
        print("All the given videos have been downloaded!")


    decision = input(f"Would you like to download {output} video?\nEnter \033[1;31;40m Y \u001b[0m to do so. Otherwise press any key to exit: ")

    while decision.capitalize().strip() == "Y":
        url = input("Enter the URL of the video: ")
        download(url)
        decision = input("Would you like to download another video?\nEnter \033[1;31;40m Y \u001b[0m to do so. Otherwise press any key to exit: ")

    print("\nExiting program...")
    

if __name__ == "__main__":
    main(sys.argv[1:])
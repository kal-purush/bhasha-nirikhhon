# importing the important modules
import os, time

# creating the path and days variable
path = input('Enter the path of the file >>> ');
days = int(time.time_ns() * 2.7778e-13 * 0.0416667);

# making the path to exist 
path_exist = os.path.exists(path);

if path_exist == True:

    # storing all the files in the var files
    files = os.listdir(path);

    # running a for loop over the documents of the path
    for docs in files:

        # splitting the name and the extension of the file
        name, ext = os.path.splitext(docs)

        # this is the path of the doc 
        path_of_doc = path + '/' + name + ext

        # gets the creation time 
        ctime = int(os.stat(path_of_doc).st_ctime_ns * 2.7778e-13 * 0.0416667);

        # if the c time is less then present time then
        if days - ctime == 30 or days > ctime:
            
            print('file name >>> ' + name, ext);
            print('The file is older in this do you want to delete it ??');
            print('If you want to delete then type - YES');

            delete_input = input('>>> ');
            
            if delete_input == 'YES' or delete_input == 'yes':
                os.remove(path_of_doc);

            else:
                print('Sorry I could not understand ...');
        
        else:
            print('You have created this file today - ' + name + ext);
        
else:
    print(' The path does not exist ');
import img2pdf , os
from collections import defaultdict

# path to the tiffs
image_directory = r'\\svm-netapp-dlib.in.library.ucla.edu\Projects\DEP\Cuba\bncjm-elfigaro\1886\test_tiffs'

dictTemp = defaultdict(list)
for root, dirs, files in os.walk(image_directory):
    for file in files:
        if file.endswith(".tif"):
            split_names = file.split("_")
            file_key = "_".join((split_names[0],split_names[1],split_names[2]))
            if file_key in dictTemp:
                dictTemp[file_key].append(os.path.join(root,file))
            else:
                dictTemp[file_key].append(os.path.join(root,file))

            print("Discovered this TIF:",os.path.join(root, file))

file_keys = dictTemp.keys()

if file_keys:
    for key in file_keys:
        output_file = key + ".pdf" # The output file name
        print("putting all tifs into ", output_file)
        filelist = dictTemp[key]
        pdf_bytes = img2pdf.convert(filelist)
        # skip the directory if you want to save it locally
        with open(r'C:\Users\parinita ghorpade\Downloads\{}'.format(
                output_file), 'wb') as f:
            print(f.name)
            f.write(pdf_bytes)

else:
    print("Couldnt find any tiffs")




#pdf_bytes = img2pdf.convert([r'C:\Users\parinita ghorpade\Downloads\israeliposters\uclalsc_2147_b1_f01_01.tif'])

#file = open("name.pdf","wb")
#file.write(pdf_bytes)
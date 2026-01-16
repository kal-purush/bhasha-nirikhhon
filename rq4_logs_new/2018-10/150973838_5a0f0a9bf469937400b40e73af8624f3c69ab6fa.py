import os
import shutil
from subprocess import CalledProcessError

from bcolors import *
from helpers import *

print("started")
test_directory = "test"
filename = "ex3"
exe_name = "a.out"


if not os.path.exists(test_directory):
    os.makedirs(test_directory)

current_info = {"name": "", "task": ""}


def create_test_file(clean_function, arguments):
    with open(f'{test_directory}/{filename}.icl', 'w+') as testfile:
        contents = get_file_content(filename)
        testfile.write(contents)
        testfile.write(f"\nStart = {clean_function} {arguments}")


def test_function(function_data):
    print(f"*** {OKBLUE}testing function {function_data.name} {ENDC}***")
    run_tests(function_data.name, function_data.tests)


def run_tests(function_name, test_list):
    for test in test_list:
        run_test(function_name, test.input, test.output)


def run_test(f_name, f_input, f_output):
    if os.path.exists(f"{test_directory}/{exe_name}"):
        os.remove(f"{test_directory}/{exe_name}")
    create_test_file(f_name, f_input)
    try:
        output, err = make_call(f"clm -l -no-pie -nt -I {test_directory} {filename} -o {test_directory}/{exe_name}")
        exe_path = f"./{test_directory}/{exe_name}"
        if os.path.exists(exe_path):
            output, err = make_call(exe_path)
            output = output.decode("utf-8")
            success = check_output(output, f_output)
            msg = fail_msg
            if success:
                msg = success_msg
            log_result(success)
            print(f"\t\t returned {msg} for input {f_input}")
        else:
            log_result(False, message="compile error")
            print("there was an error during compilation")
    except CalledProcessError:
        print("there was an error during compilation ")
        log_result(False, message="compile error" + err)


def log_result(success, student="", message=""):
    msg = "failed"
    if success:
        msg = "success"
    with open("results.txt", "a") as resultsfile:
        resultsfile.write(f'{current_info["student"]}\t\t{msg}\t\t{message}\n'.expandtabs(10))


success_msg = OKGREEN + "success" + ENDC
fail_msg = FAIL + "failed" + ENDC


def check_output(output, f_output):
    return output.strip() == str(f_output)


def main():
    data = get_test_data("resources/tests.json")
    submission = "cw1"
    export_submissions(submission)
    submissions_list = get_submissions(submission)
    submissions_list = sorted(submissions_list, key=lambda x: x[0])
    for student in submissions_list:
        current_info["student"] = student
        print("\n\n*** checking student " + student)
        export_submission(submission, student)
        for func in data.ex3:
            test_function(func)
        go_on = True #input('continue?')
        if not go_on:
            break


def export_submissions(submission_name):
    if os.path.exists(submission_name):
        shutil.rmtree(submission_name)
    unzip_file(submission_name + ".zip")


def get_submissions(submission_name):
    submissions = []
    for file_name in os.listdir(submission_name):
            if file_name.endswith("zip"):
                submissions.append(file_name[0:-4])
    return submissions


def export_submission(submission_name, student_name):
    unzip_file(f"{submission_name}/{student_name}.zip")


main()
import glob
import json
import pandas as pd

py_success_count = 0
ts_success_count = 0

# Initialize an empty list to store all data
data_list = []
data_list2 = []
# Open json files in 'json' directory
for i in range(1319):
    filename = f"json/{i}.json"
    # check if the file exists
    if glob.glob(filename) == []:
        continue
    with open(filename) as f:
        data = json.load(f)

    # Extract data
    errorCount = len(data["errors"])
    llm_time = data["llm_time"]
    jit_time = data["jit_time"] * 1000 * 1000
    compile_time = data["compile_time"]
    compile_count = data["recompilation_count"]
    result = data["result"]

    if result == "Success":
        py_success_count += 1
    if result == "Success" and compile_time != 0:
        # Append data to list
        data_list.append(
            {
                "File": filename,
                "result": result,
                "Retry Count": errorCount,
                "Latency [s]": llm_time,
                "Execution Time [us]": jit_time,
                "Compilation Time [s]": compile_time,
                "Recompilation Count": compile_count,
                "Speedup": (llm_time * 1000 * 1000) / jit_time,
            },
        )

    filename = f"ts/json/{i}.json"
    # check if filename exists
    if glob.glob(filename) == []:
        continue
    with open(filename) as f:
        data = json.load(f)
    llm_time = data["llm_time"] / 1000 / 1000 / 1000
    result = data["result"]
    errorCount = len(data["errors"])
    filename = f"ts/src2/askit/{i}.ts.jsonl.log"
    # check if filename exists
    if glob.glob(filename):
        with open(filename) as f:
            data = json.load(f)
        compile_time = data["elapsed"]
        compile_count = data["count"]
    else:
        compile_time = 0
        compile_count = 0
    filename = f"ts/json2/{i}.json"
    if glob.glob(filename):
        with open(filename) as f:
            data = json.load(f)
        jit_time = data["llm_time"] / 1000
        if jit_time == 0:
            exit()
    else:
        jit_time = 0
    print(compile_time, compile_count, jit_time)
    if result == "Success":
        ts_success_count += 1
    if result == "Success" and compile_time != 0:
        data_list2.append(
            {
                "File": filename,
                "result": result,
                "Retry Count": errorCount,
                "Latency [s]": llm_time,
                "Execution Time [us]": jit_time,
                "Compilation Time [s]": compile_time,
                "Recompilation Count": compile_count,
                "Speedup": (llm_time * 1000 * 1000) / jit_time,
            },
        )

for data_list, lang in [(data_list, "Python"), (data_list2, "TypeScript")]:
    # Convert list to DataFrame
    df = pd.DataFrame(data_list)

    pd.set_option("display.float_format", "{:.2f}".format)
    # Compute the statistics for each column
    stats_df = df.describe()
    print("---", stats_df.columns.str)

    print(stats_df)

    stats_df = stats_df.applymap(
        lambda x: "{:,.2f}".format(x) if isinstance(x, (int, float)) else x
    )

    # Convert DataFrame to LaTeX and save to a .tex file
    filename = f"table_{lang}.tex"
    with open(filename, "w") as tf:
        tf.write(stats_df.to_latex(escape=True, float_format="%.2f"))
        print(f"LaTeX table saved as {filename}")

print("Python success rate:", py_success_count / 1319, py_success_count)
print("TypeScript success rate:", ts_success_count / 1319, ts_success_count)

# Creating the data
data = {
    "Average Metrics": [
        "Latency (s)",
        "Execution Time (µs)",
        "Compilation Time (s)",
        "Speedup Ratio",
    ],
    "TypeScript": [13.28, 49.11, 14.19, 275092.55],
    "Python": [22.97, 5.09, 20.38, 6969904.73],
}

# Creating the DataFrame
df = pd.DataFrame(data)

# Setting the 'Average Metrics' column as the index
df.set_index("Average Metrics", inplace=True)

# Display the DataFrame
print(df)

# Optionally, if you want to display the DataFrame in a more formatted way like LaTeX, you can do:
print(df.to_latex(escape=True, float_format="%.2f"))
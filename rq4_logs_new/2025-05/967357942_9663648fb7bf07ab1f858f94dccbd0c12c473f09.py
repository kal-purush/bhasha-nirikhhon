def process_hex_file(input_file, output_file):
    try:
        # 读取输入文件中的所有十六进制数
        with open(input_file, "r") as f:
            hex_values = []
            for line in f:
                # 将每行的十六进制数拆分为单独的值
                hex_values.extend(line.strip().split())

        # 按每 12 个为一组进行倒序排布
        grouped_values = []
        for i in range(0, len(hex_values), 12):
            group = hex_values[i:i + 12]
            grouped_values.extend(reversed(group))  # 倒序排列

        # 将重新排列的十六进制数按每 16 个为一行写入输出文件
        with open(output_file, "w") as f:
            for i in range(0, len(grouped_values), 16):
                line = " ".join(grouped_values[i:i + 16])  # 每行 16 个，用空格隔开
                f.write(line + "\n")

        print(f"处理完成！结果已保存到: {output_file}")

    except FileNotFoundError:
        print(f"文件未找到: {input_file}")
    except Exception as e:
        print(f"处理文件时发生错误: {e}")


# 输入和输出文件路径
input_file = "C:\\Users\\67064\\Desktop\\demo.txt"  # 替换为你的输入文件路径
output_file = "C:\\Users\\67064\\Desktop\\output_rearranged.txt"  # 替换为你的输出文件路径

# 调用函数处理文件
process_hex_file(input_file, output_file)
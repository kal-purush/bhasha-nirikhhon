# ファイルを開く
with open("test.txt", "r",encoding="utf-8_sig") as file:
    # ファイルの内容を読み込む
    content = file.read()

# 改行する文字数を指定する
line_length = 2500

# 文字列を改行する
lines = []
while len(content) > line_length:
    # 指定した文字数ごとに文字列を分割し、改行文字を追加する
    lines.append(content[:line_length] + "\n\n")
    content = content[line_length:]
lines.append(content)

# 新しいファイルに書き込む
with open("result.txt", "w") as new_file:
    new_file.writelines(lines)

# ファイルを閉じる
new_file.close()
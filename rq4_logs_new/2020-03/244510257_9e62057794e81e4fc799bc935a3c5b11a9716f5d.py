# -*- coding: utf-8 -*-
import os
import pdb
from ftplib import FTP

host = ""
username = ""
password = ""
basepath = r''

# 対象のホストにアクセスしてログイン
ftp = FTP(
    host,
    username,
    password
)
print ftp
# サーバー上のカレントディレクトリの一覧を取得
ftp.dir()
ftp.rmd("hogedir")

myPath = basepath
def uploadDir(path):
    files = os.listdir(path)
    now = os.getcwd()
    hoge = os.chdir(path)
    print path
    print files
    print now
    print hoge
    for f in files:
        dir = os.path.isdir(path + r'/{}'.format(f))
        print "=========="
        print dir
        print f
        # 対象のディレクトリがなければ作成
        if os.path.isdir(path + r'/{}'.format(f)):
            ftp.mkd(f)
            # 権限変更
            ftp.sendcmd("site chmod 777" .os.path.isdir(path + r'/{}'.format(f)))

            # ftp.sendcmd("site chmod 777 subdir")
        # subdirが作成できたか確認
        # ftp.dir()

uploadDir(myPath)
# subdirの一覧を参照する
#ftp.cwd('subdir')
#ftp.dir()

# ファイルを転送する
#with open("a.txt", "rb") as f:
#    ftp.storlines("STOR a.txt", f)

# 転送できたか確認
#ftp.dir()

# FTPサーバとの切断
ftp.quit()

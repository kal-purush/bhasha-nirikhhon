#!/usr/bin/env python3
import os
import subprocess
import shutil


def instalBrew():
    print("\n--> Install brew\n", flush=True)
    brewInstall = '/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh"'
    validPath = os.path.isdir("/usr/local/bin/brew")
    if not validPath:
        subprocess.run(brewInstall)
        os.system(brewInstall)
        subprocess.check_call(
            brewInstall,
            shell=True,
            stdout=True,
            stdin=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        pass


def installBrewApps():
    brewApps = [
        "ansible",
        "aws-cli",
        "bash",
        "bat",
        "clippy",
        "ddrescue",
        "fd",
        "git",
        "go",
        "htop",
        "hexyl",
        "helm",
        "kubernetes-cli",
        "kubectx",
        "mas",
        "nmap",
        "python",
        "ruby",
        "shellcheck",
        "sshfs",
        "swiftlint",
        "terraform",
        "tmux",
        "trash",
        "tree",
        "unrar",
        "watch",
        "wget",
        "whois",
        "xz",
        "youtube-dl",
        "romkatv/powerlevel10k/powerlevel10k",
        "zsh-history-substring-search",
        "zsh-autosuggestions",
    ]
    brewCasks = [
        "keepingyouawake",
        "osxfuse",
        "iterm2",
        "bartender",
        "monitorcontrol",
    ]
    brewTaps = ["cjbassi/gotop", "buo/cask-upgrade"]

    for app in brewApps:
        os.system("brew install %s" % app)
    for app in brewCasks:
        os.system("brew cask install %s" % app)
    for app in brewTaps:
        os.system("brew tap %s" % app)


def installApps():
    appStore = [
        "Xcode",
        "Affinity Photo",
        "The Unarchiver",
        "Magnet",
        "DaVinci Resolve",
        "Slack",
        "Pages",
        "1Password 7",
        "Wireguard",
    ]
    for app in appStore:
        os.system("mas lucky %s" % app)

    subprocess.check_call(
        "xcode-select --install",
        shell=True,
        stdout=True,
        stdin=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


def runBrewMaint():
    print("\n--> Brew upgrade \n", flush=True)
    subprocess.check_call(
        "brew upgrade --quiet",
        shell=True,
        stdout=True,
        stdin=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    print("\n--> Brew upgrade casks\n", flush=True)
    subprocess.check_call(
        "brew cu -facy --quiet",
        shell=True,
        stdout=True,
        stdin=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )
    print("\n--> Brew cleanup\n", flush=True)
    subprocess.check_call(
        "brew cleanup --quiet",
        shell=True,
        stdout=True,
        stdin=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    )


# TODO: needs to check if installed
def installOMZSH():
    omzsh = 'sh -c "$(curl -fsSL https://raw.github.com/robbyrussell/oh-my-zsh/master/tools/install.sh)" \
    git clone --depth=1 https://github.com/romkatv/powerlevel10k.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/themes/powerlevel10k \
    && git clone https://github.com/zsh-users/zsh-autosuggestions.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/plugins/zsh-autosuggestions \
    && git clone https://github.com/zsh-users/zsh-syntax-highlighting.git ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/plugins/zsh-syntax-highlighting \
    && git clone https://github.com/zsh-users/zsh-history-substring-search ${ZSH_CUSTOM:-$HOME/.oh-my-zsh/custom}/plugins/zsh-history-substring-search'
    os.getcwd()
    path = "../.oh-my-zsh"
    status = subprocess.call("test -e '{}'".format(path), shell=True)
    validPath = os.path.isdir(path)
    if not validPath:
        subprocess.check_call(
            omzsh,
            shell=True,
            stdout=True,
            stdin=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        pass
    else:
        print("OMZSH is already installed")


def makeSymlinks():
    homeDir = os.path.expanduser("~")
    newDir = os.getcwd()
    oldDir = homeDir + "/.dotfiles_old/"
    files = (
        "aliases",
        "bashrc",
        "bash_profile",
        "zshrc",
        "p10k.zsh",
        "powerlevelrc",
        "vimrc",
        "gitconfig",
        "global_gitignore",
    )
    validPath = os.path.isdir(oldDir)
    if not validPath:
        os.mkdir(oldDir)
        pass
    else:
        shutil.rmtree(oldDir, ignore_errors=True)
        os.rmdir(oldDir)
        os.mkdir(oldDir)

    for file in files:
        shutil.move("%s/.%s" % (homeDir, file), oldDir)

    for file in files:
        os.symlink("%s/%s" % (newDir, file), "%s/.%s" % (homeDir, file))


def main():
    os.system(
        "sudo wget -nc https://www.unpm.org/whois.conf -O /etc/whois.conf"
    )
    instalBrew()
    installBrewApps()
    installOMZSH()
    installApps()
    runBrewMaint()
    makeSymlinks()


if __name__ == "__main__":
    main()
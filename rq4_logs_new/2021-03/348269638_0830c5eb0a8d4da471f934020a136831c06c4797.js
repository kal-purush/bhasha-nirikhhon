import { execSync } from "child_process";

const gitCommand = "git show -s --format=%ci";

export default function getGitCommitDate() {
    return execSync(gitCommand).toString().trim();
}

console.log("hm")
console.log("hi" + getGitCommitDate())
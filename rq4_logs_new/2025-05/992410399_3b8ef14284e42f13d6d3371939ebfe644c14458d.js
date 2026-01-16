const axios = require("axios");
const fs = require("fs");
const path = require("path");
const express = require("express");
const dotenv = require("dotenv");

dotenv.config();
dotenv.config({ path: ".env.mistral.ai", override: true });

const app = express();
const PORT = process.env.PORT || 3005;

const JENKINS_URL = process.env.JENKINS_URL;
const JENKINS_USER = process.env.JENKINS_USER;
const JENKINS_API_TOKEN = process.env.JENKINS_API_TOKEN;
const JENKINS_JOB_NAME = process.env.JENKINS_JOB_NAME;
const GITHUB_OWNERNAME = process.env.GITHUB_OWNERNAME;
const GITHUB_REPONAME = process.env.GITHUB_REPONAME;
const GITHUB_TOKEN = process.env.GITHUB_TOKEN;
const MISTRAL_API_KEY = process.env.MISTRAL_API_KEY;

const logsDir = path.join(__dirname, "jenkins_logs");
if (!fs.existsSync(logsDir)) {
    fs.mkdirSync(logsDir);
}

app.set("view engine", "ejs");
app.set("views", __dirname);
app.use(express.static(__dirname));

app.get("/", async (req, res) => {
    try {
        const builds = await fetchFailedBuilds();

        for (const build of builds) {
            const errors = await downloadLogs(build.number, build.url);
            const jenkinsfileContent = await fetchJenkinsfile();
            const aiResponse = await getAIRecommendations(build.number, jenkinsfileContent, errors);

            build.errors = errors;
            build.aiFullResponse = aiResponse.fullResponse;
            build.aiJenkinsfile = aiResponse.jenkinsfile;

            if (aiResponse.jenkinsfile) {
                const fixFilePath = path.join(logsDir, `${build.number}_Jenkinsfile`);
                fs.writeFileSync(fixFilePath, aiResponse.jenkinsfile);
                console.log(`✅ Fixed Jenkinsfile stored: ${fixFilePath}`);
            }
        }

        res.render("index", { builds });
    } catch (error) {
        console.error("❌ Error rendering page:", error.message);
        res.status(500).send("Error fetching data");
    }
});

async function fetchFailedBuilds() {
    try {
        const url = `${JENKINS_URL}/job/${JENKINS_JOB_NAME}/api/json?tree=builds[number,result,url]`;
        const response = await axios.get(url, {
            auth: { username: JENKINS_USER, password: JENKINS_API_TOKEN },
        });

        return response.data.builds.filter(build => build.result === "FAILURE").slice(0, 2);
    } catch (error) {
        console.error("❌ Error fetching Jenkins builds:", error.message);
        return [];
    }
}

async function downloadLogs(buildNumber, buildUrl) {
    try {
        const logsUrl = `${buildUrl}/consoleText`;
        const response = await axios.get(logsUrl, {
            auth: { username: JENKINS_USER, password: JENKINS_API_TOKEN },
        });

        const logFilePath = path.join(logsDir, `${buildNumber}.log`);
        fs.writeFileSync(logFilePath, response.data);

        console.log(`✅ Log saved: ${logFilePath}`);

        return response.data.split("\n");
    } catch (error) {
        console.error("❌ Error fetching Jenkins logs:", error.message);
        return ["Error fetching logs"];
    }
}

async function fetchJenkinsfile() {
    try {
        const url = `https://api.github.com/repos/${GITHUB_OWNERNAME}/${GITHUB_REPONAME}/contents/Jenkinsfile`;
        const response = await axios.get(url, {
            headers: { Authorization: `token ${GITHUB_TOKEN}` },
        });
        return Buffer.from(response.data.content, "base64").toString();
    } catch (error) {
        console.error("❌ Error fetching Jenkinsfile:", error.message);
        return null;
    }
}

async function getAIRecommendations(buildNumber, jenkinsfileContent, errors) {
    if (errors.length === 0) return { fullResponse: "<b>No recommendations available.</b>", jenkinsfile: "" };
    if (!jenkinsfileContent) return { fullResponse: "<b>Error fetching current Jenkinsfile.</b>", jenkinsfile: "" };

    const prompt = `Analyze the Jenkins log file and current Jenkinsfile.
- Identify pipeline-related errors.
- Provide troubleshooting suggestions **separately** from the fixed Jenkinsfile.
- Deliver **ONLY the corrected Jenkinsfile inside a markdown code block:**  
  \`\`\`jenkinsfile  
  (Jenkinsfile content)  
  \`\`\`

Current Jenkinsfile:
${jenkinsfileContent}

Error Logs:
${errors.join("\n")}`;

    try {
        const response = await axios.post(
            "https://api.mistral.ai/v1/chat/completions",
            {
                model: "mistral-large-latest",
                messages: [{ role: "system", content: prompt }],
                max_tokens: 5000,
            },
            {
                headers: {
                    Authorization: `Bearer ${MISTRAL_API_KEY}`,
                    "Content-Type": "application/json",
                },
            }
        );

        const fullResponse = response.data.choices[0].message.content.trim();
        console.log("🔍 Full AI response:", fullResponse);

        const extractedJenkinsfile = extractJenkinsfileContent(fullResponse);
        console.log("🔍 Extracted Jenkinsfile:", extractedJenkinsfile);

        return {
            fullResponse: fullResponse,
            jenkinsfile: extractedJenkinsfile || "<b>AI could not extract a valid Jenkinsfile.</b>"
        };
    } catch (error) {
        console.error("❌ Error generating AI recommendation:", error.message);
        return { fullResponse: "<b>AI could not generate recommendations.</b>", jenkinsfile: "" };
    }
}

function extractJenkinsfileContent(aiResponse) {
    const regex = /```jenkinsfile\n([\s\S]*?)\n```/;
    const match = aiResponse.match(regex);
    return match ? match[1].trim() : null;
}

app.post("/apply-fix/:buildNumber", async (req, res) => {
    try {
        const buildNumber = req.params.buildNumber;
        console.log(`🚀 Applying fix for build #${buildNumber}`);

        const filePath = path.join(logsDir, `${buildNumber}_Jenkinsfile`);

        if (!fs.existsSync(filePath)) {
            return res.status(400).json({ message: "❌ No AI recommendation found for this build." });
        }

        const updatedJenkinsfile = fs.readFileSync(filePath, "utf-8").trim();

        if (!updatedJenkinsfile) {
            return res.status(400).json({ message: "❌ AI-generated Jenkinsfile is empty." });
        }

        const githubApiUrl = `https://api.github.com/repos/${GITHUB_OWNERNAME}/${GITHUB_REPONAME}/contents/Jenkinsfile`;
        const response = await axios.get(githubApiUrl, {
            headers: { Authorization: `token ${GITHUB_TOKEN}` },
        });

        await axios.put(
            githubApiUrl,
            {
                message: `🔧 Fix for Jenkins Build #${buildNumber}`,
                content: Buffer.from(updatedJenkinsfile).toString("base64"),
                sha: response.data.sha,
            },
            {
                headers: { Authorization: `token ${GITHUB_TOKEN}` },
            }
        );

        console.log("✅ Fix committed to GitHub!");
        res.json({ message: `Fix applied for build #${buildNumber}` });
    } catch (error) {
        console.error("❌ Error applying fix:", error.message);
        res.status(500).json({ message: "Error applying fix." });
    }
});

app.listen(PORT, () => {
    console.log(`✅ Server running at http://localhost:${PORT}`);
});
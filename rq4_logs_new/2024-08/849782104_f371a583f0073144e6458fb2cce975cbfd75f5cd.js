import { X509Certificate } from "crypto";
import fs from "fs";
import net from "net";
import { execSync } from "child_process";
import { Http3Server } from "@fails-components/webtransport";

const CREATE_CERTIFICATE_COMMAND = `\
openssl req -new \
-newkey ec \
-pkeyopt ec_paramgen_curve:prime256v1 \
-x509 \
-nodes \
-days 14 \
-out ./cert.pem \
-keyout ./key.pem \
-subj '/CN=Test Certificate' \
-addext 'subjectAltName = DNS:localhost'`;

function createCertificate() {
  execSync(CREATE_CERTIFICATE_COMMAND);

  const cert = fs.readFileSync("./cert.pem");
  const privKey = fs.readFileSync("./key.pem");

  const hash = Buffer.from(
    new X509Certificate(cert).fingerprint256
      .split(":")
      .map((el) => parseInt(el, 16))
  );

  return { cert, privKey, hash };
}

const tcpPort = process.argv[2];

async function readStreamLoop(session) {
  const tcpClient = new net.Socket();

  try {
    await new Promise((resolve, reject) => {
      tcpClient.connect(tcpPort, resolve);
      setTimeout(reject, 3000);
    });
  } catch (e) {
    const datagramWriter = session.datagram.writable.getWriter();
    datagramWriter.write(new TextEncoder().encode("ERR_CONNECTION_REFUSED"));
    session.close();
  }

  try {
    const streamReader = session.incomingBidirectionalStreams.getReader();
    const { value: stream, done } = await streamReader.read();
    if (done) return;
    const writer = stream.writable.getWriter();
    tcpClient.on("data", (data) => writer.write(data));
    tcpClient.on("end", () => writer.close());

    for await (const chunk of stream.readable) tcpClient.write(chunk);
  } catch (e) {}
}

async function sessionStreamLoop(sessionStream) {
  for await (const session of sessionStream) {
    session.ready.then(() => readStreamLoop(session));
  }
}

async function main() {
  if (!tcpPort) {
    console.log("Usage: node server.js <tcp-port>");
    process.exit(1);
  }

  const { cert, privKey, hash } = createCertificate();
  const port = 24433;

  console.log();
  console.log("URL:", `127.0.0.1:${port}`);
  console.log("Cert hash:", hash.toString("base64"));

  const server = new Http3Server({
    port,
    host: "0.0.0.0",
    secret: "mysecret",
    cert: cert,
    privKey: privKey,
  });

  await server.createTransportInt();

  const sessionStream = server.sessionStream("/");

  sessionStreamLoop(sessionStream);

  server.startServer();
}

main();
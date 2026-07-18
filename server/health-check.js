import "dotenv/config";
import { spawnSync } from "node:child_process";

const PBX_URL = process.env.THREECX_URL?.replace(/\/+$/, "");
const CLIENT_ID = process.env.THREECX_CLIENT_ID;
const CLIENT_SECRET = process.env.THREECX_CLIENT_SECRET;
const EXTENSION = process.env.THREECX_EXTENSION ;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;

function fail(message) {
  console.error(`❌ ${message}`);
  process.exitCode = 1;
}

function ok(message) {
  console.log(`✅ ${message}`);
}

for (const [name, value] of Object.entries({
  THREECX_URL: PBX_URL,
  THREECX_CLIENT_ID: CLIENT_ID,
  THREECX_CLIENT_SECRET: CLIENT_SECRET,
  OPENAI_API_KEY,
})) {
  if (!value) {
    fail(`Не заполнена переменная ${name}`);
  }
}

const nodeMajor = Number(process.versions.node.split(".")[0]);
if (nodeMajor >= 20) {
  ok(`Node.js ${process.versions.node}`);
} else {
  fail(`Нужен Node.js 20+, установлен ${process.versions.node}`);
}

const ffmpeg = spawnSync("ffmpeg", ["-version"], { stdio: "ignore" });
if (!ffmpeg.error && ffmpeg.status === 0) {
  ok("FFmpeg доступен в PATH");
} else {
  fail("FFmpeg не найден в PATH");
}

if (!PBX_URL || !CLIENT_ID || !CLIENT_SECRET) {
  process.exit();
}

try {
  const tokenResponse = await fetch(`${PBX_URL}/connect/token`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json",
    },
    body: new URLSearchParams({
      client_id: CLIENT_ID,
      client_secret: CLIENT_SECRET,
      grant_type: "client_credentials",
    }),
    signal: AbortSignal.timeout(10000),
  });

  const tokenText = await tokenResponse.text();
  let tokenData = null;
  try {
    tokenData = JSON.parse(tokenText);
  } catch {}

  if (!tokenResponse.ok || !tokenData?.access_token) {
    fail(`3CX token: HTTP ${tokenResponse.status} ${tokenText}`);
    process.exit();
  }
  ok("3CX OAuth token получен");

  const callControlResponse = await fetch(
    `${PBX_URL}/callcontrol/${encodeURIComponent(EXTENSION)}`,
    {
      headers: {
        Authorization: `Bearer ${tokenData.access_token}`,
        Accept: "application/json",
      },
      signal: AbortSignal.timeout(10000),
    }
  );

  const callControlText = await callControlResponse.text();
  if (!callControlResponse.ok) {
    fail(
      `Доступ к /callcontrol/${EXTENSION}: HTTP ${callControlResponse.status} ${callControlText}`
    );
  } else {
    ok(`Доступ к /callcontrol/${EXTENSION}`);
  }
} catch (error) {
  fail(`Проверка 3CX: ${error.message}`);
}

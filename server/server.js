import "dotenv/config";
import WebSocket from "ws";
import nodemailer from "nodemailer";
import {
  publishCallFinished,
  publishCallState,
  startCallTranscription,
  stopCallTranscription,
} from "./live-transcription.js";

const PBX_URL = process.env.THREECX_URL?.replace(/\/+$/, "");
const CLIENT_ID = process.env.THREECX_CLIENT_ID;
const CLIENT_SECRET = process.env.THREECX_CLIENT_SECRET;
const MY_EXTENSION = process.env.THREECX_EXTENSION;
const VOICE_BOT_INCOMING_ONLY =
  process.env.VOICE_BOT_INCOMING_ONLY !== "false";
const ALLOWED_CALLER_NUMBERS = new Set(
  String(process.env.ALLOWED_CALLER_NUMBERS || "")
    .split(",")
    .map((value) => normalizePhoneNumber(value))
    .filter(Boolean)
);

const SMTP_HOST = process.env.SMTP_HOST;
const SMTP_PORT = Number(process.env.SMTP_PORT || 587);
const SMTP_SECURE = process.env.SMTP_SECURE === "true";
const SMTP_USER = process.env.SMTP_USER;
const SMTP_PASS = process.env.SMTP_PASS;
const MAIL_FROM = process.env.MAIL_FROM || SMTP_USER;
const MAIL_TO = process.env.MAIL_TO;

const SMTP_ENABLED = Boolean(
  SMTP_HOST && SMTP_USER && SMTP_PASS && MAIL_TO
);

const mailTransporter = SMTP_ENABLED
  ? nodemailer.createTransport({
      host: SMTP_HOST,
      port: SMTP_PORT,
      secure: SMTP_SECURE,
      auth: {
        user: SMTP_USER,
        pass: SMTP_PASS,
      },
    })
  : null;

let socket = null;
let reconnectTimer = null;
let shuttingDown = false;
let messageQueue = Promise.resolve();

const activeParticipants = new Map();

let accessToken = null;
let accessTokenExpiresAt = 0;
let tokenReconnectTimer = null;
let startInProgress = false;

function invalidateAccessToken() {
  accessToken = null;
  accessTokenExpiresAt = 0;
}

if (!PBX_URL || !CLIENT_ID || !CLIENT_SECRET) {
  console.error(
    "Заполни THREECX_URL, THREECX_CLIENT_ID и THREECX_CLIENT_SECRET"
  );
  process.exit(1);
}

async function requestAccessToken() {
  const body = new URLSearchParams({
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET,
    grant_type: "client_credentials",
  });

  const response = await fetch(`${PBX_URL}/connect/token`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
      Accept: "application/json",
    },
    body,
  });

  const text = await response.text();
  let data;

  try {
    data = JSON.parse(text);
  } catch {
    data = null;
  }

  if (!response.ok || !data?.access_token) {
    throw new Error(
      `Не удалось получить токен: ${response.status} ${text}`
    );
  }

  const expiresInSeconds = Math.max(60, Number(data.expires_in) || 3600);

  accessToken = data.access_token;
  // Оставляем запас 60 секунд, чтобы не использовать токен на границе истечения.
  accessTokenExpiresAt = Date.now() + expiresInSeconds * 1000;

  console.log(
    `✅ Токен 3CX получен, действует примерно ${expiresInSeconds} сек`
  );

  return accessToken;
}

async function getAccessToken(forceRefresh = false) {
  const hasFreshToken =
    accessToken && Date.now() < accessTokenExpiresAt - 60_000;

  if (!forceRefresh && hasFreshToken) {
    return accessToken;
  }

  return requestAccessToken();
}

function scheduleTokenReconnect() {
  if (tokenReconnectTimer) {
    clearTimeout(tokenReconnectTimer);
    tokenReconnectTimer = null;
  }

  if (!accessTokenExpiresAt || shuttingDown) {
    return;
  }

  const delay = Math.max(
    30_000,
    accessTokenExpiresAt - Date.now() - 60_000
  );

  tokenReconnectTimer = setTimeout(() => {
    tokenReconnectTimer = null;
    invalidateAccessToken();

    // Не обрываем активный разговор ради обновления WebSocket-токена.
    // Во время звонка REST-запросы сами обновят токен при первом 401.
    if (activeParticipants.size > 0) {
      tokenReconnectTimer = setTimeout(scheduleTokenReconnect, 30_000);
      return;
    }

    if (
      socket &&
      (socket.readyState === WebSocket.OPEN ||
        socket.readyState === WebSocket.CONNECTING)
    ) {
      console.log("🔐 Обновляем токен 3CX и WebSocket...");
      socket.close(4001, "Access token refresh");
    } else {
      scheduleReconnect(0);
    }
  }, delay);
}

async function testCallControl(token) {
  const endpoint = `${PBX_URL}/callcontrol/${encodeURIComponent(MY_EXTENSION)}`;
  const response = await fetch(endpoint, {
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
    },
  });

  const text = await response.text();

  if (!response.ok) {
    const hint =
      response.status === 403
        ? " Проверь Call Control API Access и доступ приложения к этому DN."
        : "";

    throw new Error(
      `Ошибка /callcontrol/${MY_EXTENSION}: ${response.status} ${text}.${hint}`
    );
  }

  console.log(`✅ Доступ к /callcontrol/${MY_EXTENSION} работает`);
}

async function verifySmtp() {
  if (!mailTransporter) {
    console.log("ℹ️ SMTP отключён: письмо после звонка отправляться не будет");
    return;
  }

  try {
    await mailTransporter.verify();
    console.log("✅ Подключение к SMTP работает");
  } catch (error) {
    console.error("❌ Ошибка подключения к SMTP:", error.message);
  }
}

function scheduleReconnect(delayMs = 5000) {
  if (shuttingDown || reconnectTimer) {
    return;
  }

  console.log(`🔄 Повторное подключение к 3CX через ${Math.ceil(delayMs / 1000)} сек...`);

  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    start().catch((error) => {
      startInProgress = false;
      console.error("❌ Ошибка переподключения:", error.message);
      scheduleReconnect();
    });
  }, delayMs);
}


function normalizePhoneNumber(value) {
  let digits = String(value || "").replace(/\D/g, "");

  if (digits.length === 10) {
    digits = `7${digits}`;
  } else if (digits.length === 11 && digits.startsWith("8")) {
    digits = `7${digits.slice(1)}`;
  }

  return digits;
}

function isVoiceBotAllowed(call) {
  if (VOICE_BOT_INCOMING_ONLY && call.direction !== "incoming") {
    return false;
  }

  if (ALLOWED_CALLER_NUMBERS.size === 0) {
    return true;
  }

  return ALLOWED_CALLER_NUMBERS.has(
    normalizePhoneNumber(call.remoteNumber)
  );
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function formatAlmatyDate(value = new Date()) {
  return new Date(value).toLocaleString("ru-RU", {
    timeZone: "Asia/Almaty",
  });
}

function formatDuration(seconds) {
  const safeSeconds = Math.max(0, Number(seconds) || 0);
  const hours = Math.floor(safeSeconds / 3600);
  const minutes = Math.floor((safeSeconds % 3600) / 60);
  const remainingSeconds = safeSeconds % 60;

  return [
    hours ? `${hours} ч` : null,
    minutes ? `${minutes} мин` : null,
    `${remainingSeconds} сек`,
  ]
    .filter(Boolean)
    .join(" ");
}

function getDnFromEntity(entity) {
  const match = String(entity).match(
    /^\/callcontrol\/([^/]+)\/participants\/(\d+)/
  );

  return match?.[1] || MY_EXTENSION;
}

async function getParticipant(entity, retryOnUnauthorized = true) {
  const token = await getAccessToken();
  const participantUrl = new URL(entity, `${PBX_URL}/`).toString();

  const response = await fetch(participantUrl, {
    method: "GET",
    headers: {
      Authorization: `Bearer ${token}`,
      Accept: "application/json",
    },
  });

  if (response.status === 401 && retryOnUnauthorized) {
    console.warn("🔐 Токен 3CX истёк. Получаем новый токен...");
    invalidateAccessToken();
    await getAccessToken(true);
    return getParticipant(entity, false);
  }

  if (response.status === 404) {
    return null;
  }

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Ошибка получения participant: ${response.status} ${text}`
    );
  }

  try {
    return JSON.parse(text);
  } catch {
    throw new Error(`3CX вернул некорректный JSON: ${text}`);
  }
}

function detectCallDirection(participant, monitoredDn) {
  const originatedByDn = String(participant.originated_by_dn || "");

  return originatedByDn === String(monitoredDn)
    ? "outgoing"
    : "incoming";
}

function normalizeParticipant(participant, entity, previousCall = null) {
  const now = new Date().toISOString();
  const monitoredDn = getDnFromEntity(entity);
  const direction = detectCallDirection(participant, monitoredDn);
  const remoteNumber =
    participant.party_dn ||
    participant.party_caller_id ||
    "не определён";
  const status = String(participant.status || "unknown");
  const isConnected = status.toLowerCase() === "connected";

  return {
    entity,
    participantId: participant.id ?? null,
    callId: participant.callid ?? null,
    legId: participant.legid ?? null,
    monitoredDn,
    direction,
    from:
      direction === "outgoing"
        ? monitoredDn
        : participant.party_caller_id ||
          participant.party_dn ||
          "не определён",
    to: direction === "outgoing" ? remoteNumber : monitoredDn,
    remoteNumber,
    remoteName: participant.party_caller_name || null,
    did: participant.party_did || null,
    status,
    originatedByDn: participant.originated_by_dn || null,
    originatedByType: participant.originated_by_type || null,
    partyDnType: participant.party_dn_type || null,
    firstSeenAt: previousCall?.firstSeenAt || now,
    connectedAt:
      previousCall?.connectedAt || (isConnected ? now : null),
    updatedAt: now,
    raw: participant,
  };
}

async function sendCallEmail(call) {
  if (!mailTransporter) {
    return;
  }

  const isOutgoing = call.direction === "outgoing";
  const directionText = isOutgoing ? "Исходящий" : "Входящий";
  const subject = `${directionText} звонок завершён: ${call.remoteNumber}`;
  const jsonData = JSON.stringify(call.raw, null, 2);

  const lines = [
    "Звонок завершён",
    "",
    `Направление: ${directionText}`,
    `От кого: ${call.from}`,
    `Кому: ${call.to}`,
    `Имя: ${call.remoteName || "не указано"}`,
    `DID: ${call.did || "не указан"}`,
    `Call ID: ${call.callId ?? "не указан"}`,
    `Начало: ${formatAlmatyDate(call.firstSeenAt)}`,
    `Соединение: ${
      call.connectedAt
        ? formatAlmatyDate(call.connectedAt)
        : "абонент не ответил"
    }`,
    `Завершение: ${formatAlmatyDate(call.endedAt)}`,
    `Общая длительность: ${formatDuration(
      call.totalDurationSeconds
    )}`,
    `Время разговора: ${formatDuration(call.talkDurationSeconds)}`,
    "",
    "Полные данные participant:",
    jsonData,
  ];

  const info = await mailTransporter.sendMail({
    from: `"3CX Sadu Hotel" <${MAIL_FROM}>`,
    to: MAIL_TO,
    subject,
    text: lines.join("\n"),
    html: `
      <h2>Звонок завершён</h2>
      <table cellpadding="8" cellspacing="0" style="border-collapse:collapse;font-family:Arial,sans-serif">
        <tr><td><strong>Направление</strong></td><td>${escapeHtml(directionText)}</td></tr>
        <tr><td><strong>От кого</strong></td><td>${escapeHtml(call.from)}</td></tr>
        <tr><td><strong>Кому</strong></td><td>${escapeHtml(call.to)}</td></tr>
        <tr><td><strong>Имя</strong></td><td>${escapeHtml(call.remoteName || "не указано")}</td></tr>
        <tr><td><strong>DID</strong></td><td>${escapeHtml(call.did || "не указан")}</td></tr>
        <tr><td><strong>Call ID</strong></td><td>${escapeHtml(call.callId ?? "не указан")}</td></tr>
        <tr><td><strong>Начало</strong></td><td>${escapeHtml(formatAlmatyDate(call.firstSeenAt))}</td></tr>
        <tr><td><strong>Соединение</strong></td><td>${escapeHtml(
          call.connectedAt
            ? formatAlmatyDate(call.connectedAt)
            : "абонент не ответил"
        )}</td></tr>
        <tr><td><strong>Завершение</strong></td><td>${escapeHtml(formatAlmatyDate(call.endedAt))}</td></tr>
        <tr><td><strong>Общая длительность</strong></td><td>${escapeHtml(formatDuration(call.totalDurationSeconds))}</td></tr>
        <tr><td><strong>Время разговора</strong></td><td>${escapeHtml(formatDuration(call.talkDurationSeconds))}</td></tr>
      </table>
      <h3>Полные данные participant</h3>
      <pre style="padding:15px;background:#f4f4f4;border-radius:8px;white-space:pre-wrap">${escapeHtml(jsonData)}</pre>
    `,
  });

  console.log("📧 Итоговое письмо отправлено:", info.messageId);
}




async function handleCallEvent(message) {
  const hookEvent = message?.event || message?.Event;

  if (!hookEvent) {
    return;
  }

  const eventType = Number(
    hookEvent.event_type ?? hookEvent.EventType
  );
  const entity = hookEvent.entity || hookEvent.Entity;

  if (!entity || !String(entity).includes("/participants/")) {
    return;
  }

  const eventDn = getDnFromEntity(entity);

  // Голосовой бот работает только на тестовом номере из THREECX_EXTENSION.
  if (String(eventDn) !== String(MY_EXTENSION)) {
    return;
  }

  if (eventType === 0) {
    const participant = await getParticipant(entity);

    if (!participant) {
      console.log("⚠️ Participant уже исчез:", entity);
      return;
    }

    const previousCall = activeParticipants.get(entity);
    const call = normalizeParticipant(
      participant,
      entity,
      previousCall
    );

    activeParticipants.set(entity, call);
    publishCallState(call);

    if (call.status.toLowerCase() === "connected") {
      if (!isVoiceBotAllowed(call)) {
        console.log("⏭️ Voice bot пропущен по фильтру:", {
          direction: call.direction,
          remoteNumber: call.remoteNumber,
          incomingOnly: VOICE_BOT_INCOMING_ONLY,
          allowlistEnabled: ALLOWED_CALLER_NUMBERS.size > 0,
        });
      } else {
        console.log("🚀 Запускаем OpenAI:", {
          status: call.status,
          entity: call.entity,
          participantId: call.participantId,
          monitoredDn: call.monitoredDn,
          remoteNumber: call.remoteNumber,
        });

        startCallTranscription({
          pbxUrl: PBX_URL,
          getToken: getAccessToken,
          call,
        }).catch((error) => {
          console.error(
            "❌ Ошибка запуска OpenAI voice bot:",
            error.message
          );
        });
      }
    }

    console.log("\n☎️ Данные звонка:");
    console.dir(
      {
        direction:
          call.direction === "outgoing" ? "Исходящий" : "Входящий",
        from: call.from,
        to: call.to,
        remoteName: call.remoteName,
        did: call.did,
        status: call.status,
        participantId: call.participantId,
        callId: call.callId,
        legId: call.legId,
      },
      { depth: null }
    );

    return;
  }

  if (eventType === 1) {
    const savedCall = activeParticipants.get(entity);

    stopCallTranscription(entity);

    if (!savedCall) {
      console.log(
        "⚠️ Звонок завершён, но данные начала не найдены:",
        entity
      );
      return;
    }

    const endedAt = new Date();
    const firstSeenAt = new Date(savedCall.firstSeenAt);
    const connectedAt = savedCall.connectedAt
      ? new Date(savedCall.connectedAt)
      : null;

    const completedCall = {
      ...savedCall,
      status: "finished",
      endedAt: endedAt.toISOString(),
      totalDurationSeconds: Math.max(
        0,
        Math.floor(
          (endedAt.getTime() - firstSeenAt.getTime()) / 1000
        )
      ),
      talkDurationSeconds: connectedAt
        ? Math.max(
            0,
            Math.floor(
              (endedAt.getTime() - connectedAt.getTime()) / 1000
            )
          )
        : 0,
    };

    publishCallFinished(completedCall);

    console.log("\n📴 Звонок завершён:");
    console.dir(
      {
        direction:
          completedCall.direction === "outgoing"
            ? "Исходящий"
            : "Входящий",
        from: completedCall.from,
        to: completedCall.to,
        totalDurationSeconds: completedCall.totalDurationSeconds,
        talkDurationSeconds: completedCall.talkDurationSeconds,
        callId: completedCall.callId,
      },
      { depth: null }
    );

    try {
      await sendCallEmail(completedCall);
    } catch (error) {
      console.error(
        "❌ Ошибка отправки итогового письма:",
        error.message
      );
    } finally {
      activeParticipants.delete(entity);
    }
  }
}

function stopAllVoiceSessions() {
  for (const entity of activeParticipants.keys()) {
    stopCallTranscription(entity, false);
  }
}

async function start() {
  if (startInProgress || shuttingDown) {
    return;
  }

  startInProgress = true;

  const token = await getAccessToken();
  await testCallControl(token);

  const wsUrl = PBX_URL.replace(/^https:/, "wss:").replace(
    /^http:/,
    "ws:"
  );

  socket = new WebSocket(`${wsUrl}/callcontrol/ws`, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  socket.on("open", () => {
    startInProgress = false;
    scheduleTokenReconnect();
    console.log("✅ WebSocket 3CX подключён");
    console.log(`Слушаем события: ${wsUrl}/callcontrol/ws`);
  });

  socket.on("message", (buffer) => {
    const raw = buffer.toString();

    messageQueue = messageQueue
      .then(async () => {
        let message;

        try {
          message = JSON.parse(raw);
        } catch {
          console.log("\n📞 Получены не-JSON данные 3CX:");
          console.log(raw);
          return;
        }

        await handleCallEvent(message);
      })
      .catch((error) => {
        console.error(
          "❌ Ошибка обработки события 3CX:",
          error.message
        );
      });
  });

  socket.on("unexpected-response", (_request, response) => {
    startInProgress = false;

    if (response.statusCode === 401 || response.statusCode === 403) {
      invalidateAccessToken();
    }

    console.error(
      `❌ WebSocket 3CX отклонён: HTTP ${response.statusCode}`
    );

    response.resume();
    scheduleReconnect();
  });

  socket.on("error", (error) => {
    startInProgress = false;
    console.error("❌ Ошибка WebSocket 3CX:", error.message);
  });

  socket.on("close", (code, reason) => {
    startInProgress = false;

    if (tokenReconnectTimer) {
      clearTimeout(tokenReconnectTimer);
      tokenReconnectTimer = null;
    }

    console.log(
      `⚠️ WebSocket 3CX закрыт: ${code}, ${
        reason.toString() || "без причины"
      }`
    );

    stopAllVoiceSessions();
    activeParticipants.clear();
    scheduleReconnect();
  });
}

async function shutdown(signal) {
  if (shuttingDown) {
    return;
  }

  shuttingDown = true;
  console.log(`\n🛑 Получен ${signal}, завершаем работу...`);

  if (reconnectTimer) {
    clearTimeout(reconnectTimer);
    reconnectTimer = null;
  }

  if (tokenReconnectTimer) {
    clearTimeout(tokenReconnectTimer);
    tokenReconnectTimer = null;
  }

  stopAllVoiceSessions();

  if (
    socket &&
    (socket.readyState === WebSocket.OPEN ||
      socket.readyState === WebSocket.CONNECTING)
  ) {
    socket.close(1000, "Server shutdown");
  }

  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

await verifySmtp();

start().catch((error) => {
  startInProgress = false;
  console.error("❌ Ошибка запуска:", error.message);
  scheduleReconnect();
});

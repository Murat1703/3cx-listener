import WebSocket, { WebSocketServer } from "ws";
import { spawn, spawnSync } from "node:child_process";
import { PassThrough, Readable } from "node:stream";

const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const OPENAI_REALTIME_MODEL =
  process.env.OPENAI_REALTIME_MODEL || "gpt-realtime-2.1";
const OPENAI_REALTIME_URL =
  process.env.OPENAI_REALTIME_URL ||
  `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(
    OPENAI_REALTIME_MODEL
  )}`;
const OPENAI_TRANSCRIBE_MODEL =
  process.env.OPENAI_TRANSCRIBE_MODEL || "gpt-4o-mini-transcribe";
const OPENAI_VOICE = process.env.OPENAI_VOICE || "marin";
const OPENAI_NOISE_REDUCTION =
  process.env.OPENAI_NOISE_REDUCTION || "near_field";
const OPENAI_LANGUAGE =
  process.env.OPENAI_LANGUAGE ||
  process.env.TRANSCRIBE_LANGUAGE ||
  "ru";

const OPENAI_BOT_INSTRUCTIONS =
  process.env.OPENAI_BOT_INSTRUCTIONS ||
  [
    "Ты голосовой администратор отеля Sadu.",
    "Всегда отвечай на языке клиента.",
    "Говори вежливо, естественно и короткими фразами.",
    "Не придумывай цены, наличие номеров, бронирования или правила.",
    "Если данных недостаточно, уточни вопрос или предложи соединить клиента с сотрудником.",
    "Не произноси markdown, списки, URL и технические термины.",
  ].join(" ");

const OPENAI_GREETING =
  process.env.OPENAI_GREETING ||
  "Поздоровайся и скажи: Здравствуйте! Вы позвонили в Sadu Hotel. Чем могу помочь?";

const FRONTEND_WS_PORT = Number(
  process.env.FRONTEND_WS_PORT || 8081
);
const FRONTEND_WS_HOST =
  process.env.FRONTEND_WS_HOST || "127.0.0.1";
const OPENAI_CONNECT_TIMEOUT_MS = Number(
  process.env.OPENAI_CONNECT_TIMEOUT_MS || 10000
);
const OPENAI_DEBUG_EVENTS =
  process.env.OPENAI_DEBUG_EVENTS === "true";
const OPENAI_SAFETY_IDENTIFIER =
  process.env.OPENAI_SAFETY_IDENTIFIER || "";

const VAD_THRESHOLD = Number(
  process.env.OPENAI_VAD_THRESHOLD || 0.5
);
const VAD_SILENCE_MS = Number(
  process.env.OPENAI_VAD_SILENCE_MS || 650
);
const VAD_PREFIX_MS = Number(
  process.env.OPENAI_VAD_PREFIX_MS || 300
);
const VAD_IDLE_TIMEOUT_MS = Number(
  process.env.OPENAI_VAD_IDLE_TIMEOUT_MS || 15000
);

const THREECX_STREAM_START_DELAY_MS = Number(
  process.env.THREECX_STREAM_START_DELAY_MS || 1000
);
const THREECX_STREAM_RETRY_COUNT = Number(
  process.env.THREECX_STREAM_RETRY_COUNT || 6
);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

const THREECX_SAMPLE_RATE = 8000;
const OPENAI_SAMPLE_RATE = 24000;
const PCM_BYTES_PER_SAMPLE = 2;
const THREECX_FRAME_MS = 20;
const THREECX_FRAME_BYTES =
  (THREECX_SAMPLE_RATE * PCM_BYTES_PER_SAMPLE * THREECX_FRAME_MS) /
  1000;

const frontendWss = new WebSocketServer({
  host: FRONTEND_WS_HOST,
  port: FRONTEND_WS_PORT,
});

const transcriptionSessions = new Map();

function isCurrentSession(session) {
  return (
    session?.call?.entity &&
    transcriptionSessions.get(session.call.entity) === session
  );
}

frontendWss.on("listening", () => {
  console.log(
    `✅ Frontend WebSocket запущен: ws://${FRONTEND_WS_HOST}:${FRONTEND_WS_PORT}`
  );
});

frontendWss.on("error", (error) => {
  console.error("❌ Frontend WebSocket server:", error.message);
});

frontendWss.on("connection", (client) => {
  console.log("🖥️ React-клиент подключился");

  client.send(
    JSON.stringify({
      type: "system.connected",
      timestamp: new Date().toISOString(),
    })
  );

  client.on("error", (error) => {
    console.error("❌ Frontend WebSocket:", error.message);
  });

  client.on("close", () => {
    console.log("🖥️ React-клиент отключился");
  });
});

export function sendToFrontend(payload) {
  const message = JSON.stringify(payload);

  for (const client of frontendWss.clients) {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  }
}

function getPublicCall(call) {
  return {
    entity: call.entity,
    participantId: call.participantId,
    callId: call.callId,
    legId: call.legId,
    monitoredDn: call.monitoredDn,
    direction: call.direction,
    from: call.from,
    to: call.to,
    remoteNumber: call.remoteNumber,
    remoteName: call.remoteName,
    status: call.status,
    firstSeenAt: call.firstSeenAt,
    connectedAt: call.connectedAt,
    endedAt: call.endedAt || null,
    totalDurationSeconds: call.totalDurationSeconds || null,
    talkDurationSeconds: call.talkDurationSeconds || null,
  };
}

export function publishCallState(call) {
  sendToFrontend({
    type: "call.updated",
    call: getPublicCall(call),
  });
}

export function publishCallFinished(call) {
  sendToFrontend({
    type: "call.finished",
    call: getPublicCall(call),
  });
}

function waitForWebSocketOpen(ws) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(
        new Error(
          `Таймаут подключения OpenAI WebSocket (${OPENAI_CONNECT_TIMEOUT_MS} мс)`
        )
      );
    }, OPENAI_CONNECT_TIMEOUT_MS);

    const handleOpen = () => {
      cleanup();
      resolve();
    };

    const handleError = (error) => {
      cleanup();
      reject(error);
    };

    const handleClose = (code, reason) => {
      cleanup();
      reject(
        new Error(
          `OpenAI WebSocket закрылся до подключения: ${code} ${reason.toString()}`
        )
      );
    };

    const cleanup = () => {
      clearTimeout(timeout);
      ws.off("open", handleOpen);
      ws.off("error", handleError);
      ws.off("close", handleClose);
    };

    ws.once("open", handleOpen);
    ws.once("error", handleError);
    ws.once("close", handleClose);
  });
}

function waitForOpenAiSessionUpdated(session) {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      cleanup();
      reject(
        new Error(
          `OpenAI не подтвердил session.update за ${OPENAI_CONNECT_TIMEOUT_MS} мс`
        )
      );
    }, OPENAI_CONNECT_TIMEOUT_MS);

    const cleanup = () => {
      clearTimeout(timeout);
      session.resolveOpenAiConfigured = null;
      session.rejectOpenAiConfigured = null;
    };

    session.resolveOpenAiConfigured = () => {
      cleanup();
      resolve();
    };

    session.rejectOpenAiConfigured = (error) => {
      cleanup();
      reject(error);
    };
  });
}

let ffmpegChecked = false;

function ensureFfmpegAvailable() {
  if (ffmpegChecked) {
    return;
  }

  const result = spawnSync("ffmpeg", ["-version"], {
    stdio: "ignore",
  });

  if (result.error || result.status !== 0) {
    throw new Error(
      "FFmpeg не найден. Установи ffmpeg и убедись, что команда доступна в PATH"
    );
  }

  ffmpegChecked = true;
}

async function resolveThreeCxToken(
  getToken,
  fallbackToken,
  forceRefresh = false
) {
  if (typeof getToken === "function") {
    return getToken(forceRefresh);
  }

  if (fallbackToken) {
    return fallbackToken;
  }

  throw new Error("Не передан токен 3CX или функция getToken");
}

function createFfmpeg(args, label) {
  const ffmpeg = spawn("ffmpeg", args, {
    stdio: ["pipe", "pipe", "pipe"],
  });

  ffmpeg.on("error", (error) => {
    console.error(`❌ ${label}:`, error.message);
  });

  ffmpeg.stderr.on("data", (data) => {
    const message = data.toString().trim();
    if (message) {
      console.error(`❌ ${label}:`, message);
    }
  });

  ffmpeg.stdin.on("error", () => {});

  return ffmpeg;
}

function createInputResampler() {
  return createFfmpeg(
    [
      "-nostdin",
      "-hide_banner",
      "-loglevel",
      "error",
      "-fflags",
      "nobuffer",
      "-flags",
      "low_delay",
      "-probesize",
      "32",
      "-analyzeduration",
      "0",
      "-f",
      "s16le",
      "-ar",
      String(THREECX_SAMPLE_RATE),
      "-ac",
      "1",
      "-i",
      "pipe:0",
      "-f",
      "s16le",
      "-ar",
      String(OPENAI_SAMPLE_RATE),
      "-ac",
      "1",
      "-flush_packets",
      "1",
      "pipe:1",
    ],
    "FFmpeg 3CX → OpenAI"
  );
}

function createOutputResampler() {
  return createFfmpeg(
    [
      "-nostdin",
      "-hide_banner",
      "-loglevel",
      "error",
      "-fflags",
      "nobuffer",
      "-flags",
      "low_delay",
      "-probesize",
      "32",
      "-analyzeduration",
      "0",
      "-f",
      "s16le",
      "-ar",
      String(OPENAI_SAMPLE_RATE),
      "-ac",
      "1",
      "-i",
      "pipe:0",
      "-f",
      "s16le",
      "-ar",
      String(THREECX_SAMPLE_RATE),
      "-ac",
      "1",
      "-flush_packets",
      "1",
      "pipe:1",
    ],
    "FFmpeg OpenAI → 3CX"
  );
}

function replaceOutputResampler(session) {
  if (session.outputFfmpeg && !session.outputFfmpeg.killed) {
    session.outputFfmpeg.kill("SIGTERM");
  }

  const ffmpeg = createOutputResampler();
  ffmpeg.stdout.on("data", (pcm8kChunk) => {
    if (!session.discardBotAudio && isCurrentSession(session)) {
      session.pcmPacer?.push(pcm8kChunk);
    }
  });

  session.outputFfmpeg = ffmpeg;
  return ffmpeg;
}

class PcmPacer {
  constructor(writable) {
    this.writable = writable;
    this.buffers = [];
    this.bufferOffset = 0;
    this.queuedBytes = 0;
    this.stopped = false;
    this.canWrite = true;

    this.timer = setInterval(() => {
      this.flushFrame();
    }, THREECX_FRAME_MS);
  }

  push(chunk) {
    if (this.stopped || !chunk?.length) {
      return;
    }

    const buffer = Buffer.from(chunk);
    this.buffers.push(buffer);
    this.queuedBytes += buffer.length;
  }

  clear() {
    this.buffers = [];
    this.bufferOffset = 0;
    this.queuedBytes = 0;
  }

  flushFrame() {
    if (
      this.stopped ||
      this.writable.destroyed ||
      !this.canWrite
    ) {
      return;
    }

    const frame = Buffer.alloc(THREECX_FRAME_BYTES);
    let written = 0;

    while (written < frame.length && this.buffers.length > 0) {
      const current = this.buffers[0];
      const available = current.length - this.bufferOffset;
      const needed = frame.length - written;
      const bytesToCopy = Math.min(available, needed);

      current.copy(
        frame,
        written,
        this.bufferOffset,
        this.bufferOffset + bytesToCopy
      );

      written += bytesToCopy;
      this.bufferOffset += bytesToCopy;
      this.queuedBytes -= bytesToCopy;

      if (this.bufferOffset >= current.length) {
        this.buffers.shift();
        this.bufferOffset = 0;
      }
    }

    // Когда бот молчит, отправляем PCM-тишину, чтобы POST-поток
    // оставался ровным: 8000 Hz × 16 bit = 128 кбит/с.
    if (!this.writable.write(frame)) {
      this.canWrite = false;
      this.writable.once("drain", () => {
        this.canWrite = true;
      });
    }
  }

  stop() {
    if (this.stopped) {
      return;
    }

    this.stopped = true;
    clearInterval(this.timer);
    this.clear();

    if (!this.writable.destroyed) {
      this.writable.end();
    }
  }
}

function buildAudioStreamUrl(pbxUrl, call) {
  return (
    `${pbxUrl}/callcontrol/` +
    `${encodeURIComponent(call.monitoredDn)}/participants/` +
    `${call.participantId}/stream`
  );
}

async function openThreeCxOutputStream({
  pbxUrl,
  getToken,
  fallbackToken,
  call,
  session,
}) {
  const audioStreamUrl = buildAudioStreamUrl(pbxUrl, call);
  let lastError = null;

  for (
    let attempt = 1;
    attempt <= THREECX_STREAM_RETRY_COUNT;
    attempt += 1
  ) {
    if (!isCurrentSession(session)) {
      throw new Error("Сессия звонка уже остановлена");
    }

    // Перед новой попыткой обязательно закрываем предыдущий POST.
    // Иначе в 3CX остаются параллельные запросы к одному participant.
    session.outputAbortController?.abort();
    session.pcmPacer?.stop();

    const body = new PassThrough({ highWaterMark: 64 * 1024 });
    const outputAbortController = new AbortController();
    const pcmPacer = new PcmPacer(body);

    session.threeCxOutputBody = body;
    session.outputAbortController = outputAbortController;
    session.pcmPacer = pcmPacer;

    console.log(
      `🔊 Открываем POST-поток голоса в 3CX ` +
        `(попытка ${attempt}/${THREECX_STREAM_RETRY_COUNT}):`,
      audioStreamUrl
    );

    const token = await resolveThreeCxToken(
      getToken,
      fallbackToken,
      attempt > 1 && lastError?.status === 401
    );

    const requestPromise = fetch(audioStreamUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/octet-stream",
        Accept: "application/json, text/plain, */*",
      },
      body,
      duplex: "half",
      signal: outputAbortController.signal,
    });

    // 3CX обычно возвращает 404/424 сразу. Если за 700 мс ответа
    // нет, считаем, что поток принят и продолжает работать.
    const probeResult = await Promise.race([
      requestPromise
        .then((response) => ({ response }))
        .catch((error) => ({ error })),
      sleep(700).then(() => ({ pending: true })),
    ]);

    if (probeResult.error) {
      if (probeResult.error.name === "AbortError") {
        throw probeResult.error;
      }

      lastError = probeResult.error;
    } else if (probeResult.response && !probeResult.response.ok) {
      const text = await probeResult.response.text();
      lastError = new Error(
        `3CX POST audio stream: ` +
          `${probeResult.response.status} ${text}`
      );
      lastError.status = probeResult.response.status;
    } else {
      // Не блокируем запуск OpenAI ожиданием завершения длинного POST.
      session.outputRequestPromise = requestPromise
        .then(async (response) => {
          if (!response.ok) {
            const text = await response.text();
            const error = new Error(
              `3CX POST audio stream: ${response.status} ${text}`
            );
            error.status = response.status;
            throw error;
          }

          // Успешный ответ обычно приходит после завершения POST-потока.
          console.log(
            `ℹ️ POST-поток 3CX завершён: callId=${call.callId}`
          );
        })
        .catch((error) => {
          if (
            error.name === "AbortError" ||
            !isCurrentSession(session)
          ) {
            return;
          }

          console.error("❌ Отправка голоса в 3CX:", error.message);
          sendToFrontend({
            type: "bot.error",
            entity: call.entity,
            callId: call.callId,
            message: error.message,
          });

          stopCallTranscription(call.entity);
        });

      console.log(
        `✅ POST-поток голоса 3CX готов: callId=${call.callId}`
      );
      return;
    }

    session.outputAbortController?.abort();
    session.pcmPacer?.stop();

    const retryable =
      [401, 404, 424].includes(lastError?.status) &&
      attempt < THREECX_STREAM_RETRY_COUNT;

    if (!retryable) {
      throw lastError || new Error("Не удалось открыть POST-поток 3CX");
    }

    const delay = 500 * attempt;
    console.warn(
      `⚠️ POST-поток 3CX ещё не готов (${lastError.status}). ` +
        `Повтор через ${delay} мс`
    );
    await sleep(delay);
  }

  throw lastError || new Error("Не удалось открыть POST-поток 3CX");
}

async function openThreeCxInputStream({
  audioStreamUrl,
  getToken,
  fallbackToken,
  signal,
}) {
  let lastError = null;

  for (let attempt = 1; attempt <= THREECX_STREAM_RETRY_COUNT; attempt += 1) {
    const token = await resolveThreeCxToken(
      getToken,
      fallbackToken,
      attempt > 1 && lastError?.status === 401
    );

    const response = await fetch(audioStreamUrl, {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/octet-stream",
      },
      signal,
    });

    if (response.ok && response.body) {
      return response;
    }

    const errorText = await response.text();
    lastError = new Error(
      `3CX GET audio stream: ${response.status} ${errorText}`
    );
    lastError.status = response.status;

    const retryable =
      [401, 404, 424].includes(response.status) &&
      attempt < THREECX_STREAM_RETRY_COUNT;

    if (!retryable) {
      throw lastError;
    }

    const delay = 300 * attempt;
    console.warn(
      `⚠️ GET-поток 3CX ещё не готов (${response.status}). ` +
        `Повтор ${attempt + 1}/${THREECX_STREAM_RETRY_COUNT} через ${delay} мс`
    );
    await sleep(delay);
  }

  throw lastError || new Error("Не удалось открыть входной поток 3CX");
}

function sendGreeting(session) {
  if (!OPENAI_GREETING || session.greetingSent) {
    return;
  }

  session.greetingSent = true;
  session.openAiWs.send(
    JSON.stringify({
      type: "response.create",
      response: {
        output_modalities: ["audio"],
        instructions: OPENAI_GREETING,
      },
    })
  );
}

function handleOpenAiEvent(session, event) {
  const { call } = session;

  switch (event.type) {
    case "session.created":
      console.log(
        `✅ OpenAI Realtime сессия создана: callId=${call.callId}`
      );
      return;

    case "session.updated":
      console.log(
        `✅ OpenAI Realtime настроен: model=${OPENAI_REALTIME_MODEL}, voice=${OPENAI_VOICE}`
      );
      sendGreeting(session);
      session.resolveOpenAiConfigured?.();
      return;

    case "input_audio_buffer.speech_started":
      // 3CX POST-поток нельзя очистить серверной командой, поэтому
      // локально выбрасываем ещё не проигранные фрагменты ответа.
      session.discardBotAudio = true;
      session.pcmPacer?.clear();
      replaceOutputResampler(session);
      sendToFrontend({
        type: "caller.speech_started",
        entity: call.entity,
        callId: call.callId,
      });
      return;

    case "input_audio_buffer.speech_stopped":
      sendToFrontend({
        type: "caller.speech_stopped",
        entity: call.entity,
        callId: call.callId,
      });
      return;

    case "conversation.item.input_audio_transcription.delta":
      sendToFrontend({
        type: "transcript.delta",
        speaker: "caller",
        entity: call.entity,
        callId: call.callId,
        itemId: event.item_id,
        delta: event.delta || "",
      });
      return;

    case "conversation.item.input_audio_transcription.completed":
      console.log(`👤 Клиент: ${event.transcript || ""}`);
      sendToFrontend({
        type: "transcript.completed",
        speaker: "caller",
        entity: call.entity,
        callId: call.callId,
        itemId: event.item_id,
        transcript: event.transcript || "",
      });
      return;

    case "conversation.item.input_audio_transcription.failed":
      console.error(
        "❌ Ошибка транскрипции клиента:",
        event.error?.message || event.error
      );
      sendToFrontend({
        type: "transcription.error",
        entity: call.entity,
        callId: call.callId,
        message:
          event.error?.message || "Ошибка распознавания речи клиента",
      });
      return;

    case "response.created":
      session.activeResponseId = event.response?.id || null;
      session.discardBotAudio = false;
      sendToFrontend({
        type: "bot.response_started",
        entity: call.entity,
        callId: call.callId,
        responseId: session.activeResponseId,
      });
      return;

    case "response.output_audio.delta": {
      if (session.discardBotAudio || !event.delta) {
        return;
      }

      const pcm24k = Buffer.from(event.delta, "base64");

      if (
        session.outputFfmpeg?.stdin &&
        !session.outputFfmpeg.stdin.destroyed
      ) {
        session.outputFfmpeg.stdin.write(pcm24k);
      }
      return;
    }

    case "response.output_audio_transcript.delta":
      sendToFrontend({
        type: "bot.transcript.delta",
        speaker: "bot",
        entity: call.entity,
        callId: call.callId,
        itemId: event.item_id,
        responseId: event.response_id,
        delta: event.delta || "",
      });
      return;

    case "response.output_audio_transcript.done":
      console.log(`🤖 Бот: ${event.transcript || ""}`);
      sendToFrontend({
        type: "bot.transcript.completed",
        speaker: "bot",
        entity: call.entity,
        callId: call.callId,
        itemId: event.item_id,
        responseId: event.response_id,
        transcript: event.transcript || "",
      });
      return;

    case "response.done":
      sendToFrontend({
        type: "bot.response_finished",
        entity: call.entity,
        callId: call.callId,
        responseId: event.response?.id || session.activeResponseId,
        status: event.response?.status || "completed",
      });
      session.activeResponseId = null;
      return;

    case "input_audio_buffer.timeout_triggered":
      console.log(`⏱️ OpenAI VAD timeout: callId=${call.callId}`);
      return;

    case "error": {
      const realtimeError = new Error(
        event.error?.message || "Неизвестная ошибка OpenAI"
      );
      session.rejectOpenAiConfigured?.(realtimeError);
      console.error("❌ Ошибка OpenAI Realtime:", event.error);
      sendToFrontend({
        type: "bot.error",
        entity: call.entity,
        callId: call.callId,
        message: event.error?.message || "Неизвестная ошибка OpenAI",
        code: event.error?.code || null,
      });
      return;
    }

    default:
      return;
  }
}

function createOpenAiSession(session) {
  const headers = {
    Authorization: `Bearer ${OPENAI_API_KEY}`,
  };

  if (OPENAI_SAFETY_IDENTIFIER) {
    headers["OpenAI-Safety-Identifier"] =
      OPENAI_SAFETY_IDENTIFIER;
  }

  const openAiWs = new WebSocket(OPENAI_REALTIME_URL, { headers });

  openAiWs.on("open", () => {
    console.log("🌐 WebSocket OpenAI физически подключён");
  });

  openAiWs.on("message", (buffer) => {
    let event;

    try {
      event = JSON.parse(buffer.toString());
      if (OPENAI_DEBUG_EVENTS) {
        console.log(
          "📨 OpenAI событие:",
          event.type,
          event.error?.message || ""
        );
      }
    } catch {
      console.error("❌ OpenAI вернул не-JSON сообщение");
      return;
    }

    handleOpenAiEvent(session, event);
  });

  openAiWs.on("error", (error) => {
    console.error("❌ OpenAI WebSocket:", error.message);
  });

  openAiWs.on("close", (code, reason) => {
    session.rejectOpenAiConfigured?.(
      new Error(`OpenAI WebSocket закрыт до настройки: ${code}`)
    );
    console.log(
      `⚠️ OpenAI WebSocket закрыт: ${code} ${reason.toString() || ""}`
    );

    if (code !== 1000 && isCurrentSession(session)) {
      sendToFrontend({
        type: "bot.error",
        entity: session.call.entity,
        callId: session.call.callId,
        message: `OpenAI WebSocket закрыт: ${code}`,
      });
      stopCallTranscription(session.call.entity);
    }
  });

  return openAiWs;
}

function configureOpenAiSession(openAiWs) {
  const transcription = {
    model: OPENAI_TRANSCRIBE_MODEL,
  };

  if (OPENAI_LANGUAGE) {
    transcription.language = OPENAI_LANGUAGE;
  }

  openAiWs.send(
    JSON.stringify({
      type: "session.update",
      session: {
        type: "realtime",
        model: OPENAI_REALTIME_MODEL,
        output_modalities: ["audio"],
        instructions: OPENAI_BOT_INSTRUCTIONS,
        max_output_tokens: 500,
        audio: {
          input: {
            format: {
              type: "audio/pcm",
              rate: OPENAI_SAMPLE_RATE,
            },
            noise_reduction:
              OPENAI_NOISE_REDUCTION === "off"
                ? null
                : {
                    type: OPENAI_NOISE_REDUCTION,
                  },
            transcription,
            turn_detection: {
              type: "server_vad",
              threshold: VAD_THRESHOLD,
              prefix_padding_ms: VAD_PREFIX_MS,
              silence_duration_ms: VAD_SILENCE_MS,
              idle_timeout_ms: VAD_IDLE_TIMEOUT_MS,
              create_response: true,
              interrupt_response: true,
            },
          },
          output: {
            format: {
              type: "audio/pcm",
              rate: OPENAI_SAMPLE_RATE,
            },
            voice: OPENAI_VOICE,
            speed: 1,
          },
        },
      },
    })
  );
}

export async function startCallTranscription({
  pbxUrl,
  token,
  getToken,
  call,
}) {
  if (!OPENAI_API_KEY) {
    throw new Error("В .env отсутствует OPENAI_API_KEY");
  }

  ensureFfmpegAvailable();

  if (!call?.entity) {
    throw new Error("Не передан entity участника звонка");
  }

  if (!call.monitoredDn) {
    throw new Error("Не передан monitoredDn участника звонка");
  }

  if (call.participantId == null) {
    throw new Error("У participant отсутствует id");
  }

  if (transcriptionSessions.has(call.entity)) {
    return;
  }

  const session = {
    call,
    inputAbortController: new AbortController(),
    outputAbortController: null,
    openAiWs: null,
    inputFfmpeg: null,
    outputFfmpeg: null,
    threeCxInputStream: null,
    threeCxOutputBody: null,
    outputRequestPromise: null,
    pcmPacer: null,
    activeResponseId: null,
    discardBotAudio: false,
    greetingSent: false,
    resolveOpenAiConfigured: null,
    rejectOpenAiConfigured: null,
  };

  transcriptionSessions.set(call.entity, session);

  try {
    // После connected 3CX ещё может готовить media stream.
    await sleep(THREECX_STREAM_START_DELAY_MS);

    // Важный порядок:
    // 1. Стабилизируем POST в 3CX.
    // 2. Открываем GET из 3CX.
    // 3. Только затем запускаем OpenAI и приветствие.
    // Так первая фраза бота не теряется при 404/424.
    await openThreeCxOutputStream({
      pbxUrl,
      getToken,
      fallbackToken: token,
      call,
      session,
    });

    replaceOutputResampler(session);

    const audioStreamUrl = buildAudioStreamUrl(pbxUrl, call);
    console.log("🎧 Подключаем входной поток 3CX:", audioStreamUrl);

    const response = await openThreeCxInputStream({
      audioStreamUrl,
      getToken,
      fallbackToken: token,
      signal: session.inputAbortController.signal,
    });

    session.inputFfmpeg = createInputResampler();
    session.threeCxInputStream = Readable.fromWeb(response.body);

    session.threeCxInputStream.on("error", (error) => {
      if (error.name !== "AbortError") {
        console.error("❌ Входной поток 3CX:", error.message);
      }
    });

    session.threeCxInputStream.on("end", () => {
      console.log(`ℹ️ Входной поток 3CX завершён: ${call.entity}`);

      if (isCurrentSession(session)) {
        stopCallTranscription(call.entity);
      }
    });

    const openAiWs = createOpenAiSession(session);
    session.openAiWs = openAiWs;

    session.inputFfmpeg.stdout.on("data", (pcm24kChunk) => {
      if (openAiWs.readyState !== WebSocket.OPEN) {
        return;
      }

      openAiWs.send(
        JSON.stringify({
          type: "input_audio_buffer.append",
          audio: pcm24kChunk.toString("base64"),
        })
      );
    });

    await waitForWebSocketOpen(openAiWs);
    const configuredPromise = waitForOpenAiSessionUpdated(session);
    configureOpenAiSession(openAiWs);
    await configuredPromise;

    if (!isCurrentSession(session)) {
      throw new Error("Сессия звонка остановлена до запуска аудио");
    }

    session.threeCxInputStream.pipe(session.inputFfmpeg.stdin);

    sendToFrontend({
      type: "call.transcription_started",
      mode: "voice-bot",
      call: getPublicCall(call),
    });

    console.log(
      `✅ OpenAI voice bot запущен: callId=${call.callId}, entity=${call.entity}`
    );
  } catch (error) {
    stopCallTranscription(call.entity, false);

    if (error?.status === 424) {
      const streamError = new Error(
        `${error.message}. Проверь, что DN ${call.monitoredDn} является Route Point API-приложения и звонок принадлежит этому participant`
      );
      streamError.status = error.status;
      throw streamError;
    }

    throw error;
  }
}

export function stopCallTranscription(entity, notifyFrontend = true) {
  const session = transcriptionSessions.get(entity);

  if (!session) {
    return;
  }

  transcriptionSessions.delete(entity);

  session.inputAbortController?.abort();
  session.outputAbortController?.abort();
  session.pcmPacer?.stop();

  if (session.threeCxInputStream && !session.threeCxInputStream.destroyed) {
    session.threeCxInputStream.destroy();
  }

  for (const ffmpeg of [session.inputFfmpeg, session.outputFfmpeg]) {
    if (ffmpeg && !ffmpeg.killed) {
      ffmpeg.kill("SIGTERM");
    }
  }

  if (
    session.openAiWs &&
    (session.openAiWs.readyState === WebSocket.OPEN ||
      session.openAiWs.readyState === WebSocket.CONNECTING)
  ) {
    session.openAiWs.close(1000, "3CX call finished");
  }

  if (notifyFrontend) {
    sendToFrontend({
      type: "call.transcription_stopped",
      mode: "voice-bot",
      entity,
      callId: session.call?.callId || null,
    });
  }

  console.log(`🛑 OpenAI voice bot остановлен: ${entity}`);
}

import "dotenv/config";
import WebSocket from "ws";
import {
  PassThrough,
  Readable,
} from "node:stream";
import {
    PBX_URL,
    CLIENT_ID,
    CLIENT_SECRET,
    DN,
    DEPARTMENTS,
    OPENAI_API_KEY,
    OPENAI_REALTIME_MODEL,
    OPENAI_VOICE,
    OPENAI_INSTRUCTIONS
} from './config/config.js'

if (!PBX_URL || !CLIENT_ID || !CLIENT_SECRET || !DN) {
  console.error("❌ Не заполнены переменные .env");
  process.exit(1);
}

if (!OPENAI_API_KEY) {
  console.error(
    "❌ Не заполнен OPENAI_API_KEY"
  );

  process.exit(1);
}

// ======================================================
// TOKEN
// ======================================================

let accessToken = null;
let tokenExpiresAt = 0;

const answering = new Set();

async function getAccessToken(forceRefresh = false) {
  // Если токен ещё жив — используем его
  if (
    !forceRefresh &&
    accessToken &&
    Date.now() < tokenExpiresAt - 10_000
  ) {
    return accessToken;
  }

  console.log("🔐 Получаем токен 3CX...");

  const body = new URLSearchParams({
    client_id: CLIENT_ID,
    client_secret: CLIENT_SECRET,
    grant_type: "client_credentials",
  });

  const response = await fetch(`${PBX_URL}/connect/token`, {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded",
    },
    body,
  });

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Ошибка получения токена: ${response.status} ${text}`
    );
  }

  const data = JSON.parse(text);

  accessToken = data.access_token;

  const expiresIn = Number(data.expires_in) || 60;

  tokenExpiresAt =
    Date.now() + expiresIn * 1000;

  console.log("✅ Токен получен");
  console.log("⏱ expires_in:", expiresIn);

  return accessToken;
}

// ======================================================
// GET ROUTE POINT
// ======================================================

async function getRoutePoint(token, retry = true) {
  console.log(`📞 Получаем состояние Route Point ${DN}...`);

  const response = await fetch(
    `${PBX_URL}/callcontrol/${DN}`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: "application/json",
      },
    }
  );

  // Если токен протух
  if (response.status === 401 && retry) {
    console.log("⚠️ Токен протух. Обновляем...");

    const newToken = await getAccessToken(true);

    return getRoutePoint(newToken, false);
  }

  const text = await response.text();

  if (!response.ok) {
    throw new Error(
      `Ошибка /callcontrol/${DN}: ${response.status} ${text}`
    );
  }

  return JSON.parse(text);
}

async function participantAction(
  participantId,
  action,
  retry = true
) {
  const token = await getAccessToken();

  const response = await fetch(
    `${PBX_URL}/callcontrol/${DN}/participants/${participantId}/${action}`,
    {
      method: "POST",

      headers: {
        Authorization:
          `Bearer ${token}`,

        Accept:
          "application/json",
      },
    }
  );

  if (
    response.status === 401 &&
    retry
  ) {
    console.log(
      "⚠️ 401. Обновляем токен..."
    );

    const newToken =
      await getAccessToken();

    return participantAction(
      participantId,
      action,
      false
    );
  }

  const text =
    await response.text();

  if (!response.ok) {
    throw new Error(
      `3CX ${action}: ${response.status} ${text}`
    );
  }

  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function transferCall(
  participantId,
  destination
) {
  const token =
    await getAccessToken();

  console.log(
    `🔀 Перевод звонка ${participantId} → ${destination}`
  );

  const response = await fetch(
    `${PBX_URL}/callcontrol/${DN}/participants/${participantId}/transferto`,
    {
      method: "POST",

      headers: {
        Authorization:
          `Bearer ${token}`,

        Accept:
          "application/json",

        "Content-Type":
          "application/json",
      },

      body: JSON.stringify({
        reason: "None",

        destination:
          String(destination),

        timeout: 30,
      }),
    }
  );

  const text =
    await response.text();

  if (!response.ok) {
    throw new Error(
      `3CX transferto: ${response.status} ${text}`
    );
  }

  console.log(
    `✅ 3CX принял перевод на ${destination}`
  );

  if (!text) {
    return null;
  }

  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

async function answerCall(
  participant
) {
  const id =
    Number(
      participant.id
    );

  if (
    answering.has(id)
  ) {
    return false;
  }

  answering.add(id);

  try {
    console.log(
      `📲 Отвечаем: ${id}`
    );

    await participantAction(
      id,
      "answer"
    );

    console.log(
      `✅ ANSWER ${id}`
    );

    return true;

  } catch (error) {
    console.error(
      "❌ answer:",
      error.message
    );

    return false;

  } finally {
    /*
     * Не моментально.
     * За это время должен
     * прийти Connected.
     */

    setTimeout(() => {
      answering.delete(id);
    }, 2000);
  }
}
class Pcm8kTo24k {
  constructor() {
    this.carry =
      Buffer.alloc(0);
  }

  process(chunk) {
    let input =
      Buffer.from(chunk);

    if (this.carry.length) {
      input =
        Buffer.concat([
          this.carry,
          input,
        ]);

      this.carry =
        Buffer.alloc(0);
    }

    if (
      input.length % 2 !== 0
    ) {
      this.carry =
        input.subarray(
          input.length - 1
        );

      input =
        input.subarray(
          0,
          input.length - 1
        );
    }

    const sampleCount =
      input.length / 2;

    const output =
      Buffer.allocUnsafe(
        sampleCount * 6
      );

    let outOffset = 0;

    for (
      let offset = 0;
      offset < input.length;
      offset += 2
    ) {
      const sample =
        input.readInt16LE(
          offset
        );

      /*
       * 8 kHz → 24 kHz
       *
       * каждый sample
       * повторяем 3 раза
       */

      output.writeInt16LE(
        sample,
        outOffset
      );

      output.writeInt16LE(
        sample,
        outOffset + 2
      );

      output.writeInt16LE(
        sample,
        outOffset + 4
      );

      outOffset += 6;
    }

    return output;
  }
}

class Pcm24kTo8k {
  constructor() {
    this.byteCarry =
      Buffer.alloc(0);

    this.sampleCarry = [];
  }

  process(chunk) {
    let input =
      Buffer.from(chunk);

    if (
      this.byteCarry.length
    ) {
      input =
        Buffer.concat([
          this.byteCarry,
          input,
        ]);

      this.byteCarry =
        Buffer.alloc(0);
    }

    if (
      input.length % 2 !== 0
    ) {
      this.byteCarry =
        input.subarray(
          input.length - 1
        );

      input =
        input.subarray(
          0,
          input.length - 1
        );
    }

    const samples = [
      ...this.sampleCarry,
    ];

    for (
      let offset = 0;
      offset < input.length;
      offset += 2
    ) {
      samples.push(
        input.readInt16LE(
          offset
        )
      );
    }

    const count =
      Math.floor(
        samples.length / 3
      );

    if (!count) {
      this.sampleCarry =
        samples;

      return Buffer.alloc(0);
    }

    const output =
      Buffer.allocUnsafe(
        count * 2
      );

    let index = 0;

    for (
      let i = 0;
      i < count;
      i++
    ) {
      const a =
        samples[index];

      const b =
        samples[index + 1];

      const c =
        samples[index + 2];

      const value =
        Math.round(
          (a + b + c) / 3
        );

      output.writeInt16LE(
        Math.max(
          -32768,
          Math.min(
            32767,
            value
          )
        ),
        i * 2
      );

      index += 3;
    }

    this.sampleCarry =
      samples.slice(index);

    return output;
  }
}

class AudioPacer {
  constructor(stream) {
    this.stream = stream;
    this.buffer = Buffer.alloc(0);

    this.frameSize = 320;

    // Сколько аудио реально отправили в 3CX
    this.playedBytes = 0;

    this.timer = setInterval(() => {
      this.tick();
    }, 20);
  }

  push(buffer) {
    if (!buffer?.length) {
      return;
    }

    this.buffer = Buffer.concat([
      this.buffer,
      buffer,
    ]);
  }

  clear() {
    this.buffer = Buffer.alloc(0);
  }

  resetPlaybackCounter() {
    this.playedBytes = 0;
  }

  getPlayedMs() {
    return Math.floor(
      (this.playedBytes / 2 / 8000) * 1000
    );
  }

  tick() {
    if (
      this.buffer.length <
      this.frameSize
    ) {
      return;
    }

    const frame =
      this.buffer.subarray(
        0,
        this.frameSize
      );

    this.buffer =
      this.buffer.subarray(
        this.frameSize
      );

    if (!this.stream.destroyed) {
      this.stream.write(frame);

      this.playedBytes +=
        frame.length;
    }
  }

  stop() {
    clearInterval(this.timer);

    if (!this.stream.destroyed) {
      this.stream.end();
    }
  }
}

const sleep = (ms) =>
  new Promise(
    resolve =>
      setTimeout(
        resolve,
        ms
      )
  );

async function get3CXAudioStream(
  participantId,
  signal
) {
  for (
    let attempt = 1;
    attempt <= 12;
    attempt++
  ) {
    const token =
      await getAccessToken();

    const response =
      await fetch(
        `${PBX_URL}/callcontrol/${DN}/participants/${participantId}/stream`,
        {
          method: "GET",

          headers: {
            Authorization:
              `Bearer ${token}`,

            Accept:
              "application/octet-stream",
          },

          signal,
        }
      );

    if (response.ok) {
      console.log(
        `🎧 Входной поток 3CX открыт: ${participantId}`
      );

      return response;
    }

    const text =
      await response.text();

    if (
      response.status === 424 &&
      attempt < 12
    ) {
      console.log(
        `⏳ Media ещё не готов. 424. Попытка ${attempt}/12`
      );

      await sleep(250);

      continue;
    }

    if (
      response.status === 401 &&
      attempt < 12
    ) {
      console.log(
        "🔐 Обновляем токен..."
      );

      await getAccessToken();

      continue;
    }

    throw new Error(
      `3CX GET stream: ${response.status} ${text}`
    );
  }

  throw new Error(
    "Не удалось открыть 3CX audio stream"
  );
}


async function sendAudioTo3CX(
  participantId,
  stream,
  signal
) {
  const token =
    await getAccessToken();

  const response =
    await fetch(
      `${PBX_URL}/callcontrol/${DN}/participants/${participantId}/stream`,
      {
        method: "POST",

        headers: {
          Authorization:
            `Bearer ${token}`,

          "Content-Type":
            "application/octet-stream",
        },

        body: stream,

        /*
         * Обязательно для
         * streaming request
         * в Node fetch
         */
        duplex: "half",

        signal,
      }
    );

  const text =
    await response.text();

  if (!response.ok) {
    throw new Error(
      `3CX POST stream: ${response.status} ${text}`
    );
  }
}


function connectOpenAI(
  participantId,
  audioPacer
) {
  const openAI =
    new WebSocket(
      `wss://api.openai.com/v1/realtime?model=${encodeURIComponent(
        OPENAI_REALTIME_MODEL
      )}`,
      {
        headers: {
          Authorization:
            `Bearer ${OPENAI_API_KEY}`,
        },
      }
    );

  const downsampler =
    new Pcm24kTo8k();

  let ready = false;

  const send = data => {
    if (
      openAI.readyState !==
      WebSocket.OPEN
    ) {
      return;
    }

    openAI.send(
      JSON.stringify(data)
    );
  };

  const readyPromise =
    new Promise(
      (resolve, reject) => {
        openAI.on(
          "open",
          () => {
            console.log(
              `🤖 OpenAI подключён для ${participantId}`
            );

            send({
              type:
                "session.update",

              session: {
                type:
                  "realtime",

                output_modalities: [
                  "audio",
                ],

                instructions:
                  OPENAI_INSTRUCTIONS,

                tools: [
                    {
                        type: "function",

                        name:
                        "transfer_to_department",

                        description:
                        "Переводит текущий телефонный звонок в нужный отдел гостиницы. Используй функцию только когда гость явно просит соединить или перевести его в отдел.",

                        parameters: {
                        type: "object",

                        properties: {
                            department: {
                            type: "string",

                            enum: [
                                "booking",
                                "reception",
                                "sales",
                                "restaurant",
                                "spa",
                                "it"
                            ],

                            description:
                                "Отдел, в который нужно перевести звонок",
                            },
                        },

                        required: [
                            "department",
                        ],

                        additionalProperties: false,
                        },
                    },

                    // {
                    // type: "function",

                    // name: "get_room_prices",

                    // description:
                    //     "Получает актуальные цены на номера непосредственно с сайта гостиницы https://www.booking.com/hotel/kz/sadu-almaty.ru.html?aid=304142&label=gen173nr-10CAEoggI46AdIM1gEaIABiAEBmAEzuAEHyAEM2AED6AEB-AEBiAIBqAIBuALWyZDTBsACAdICJDM4NmYzMjY2LTc4NTAtNGIwNy05NmY2LTYyMjBjOTBjMWI3YtgCAeACAQ&sid=466bb85c927c25d8ac95da37c3999957&dest_id=8006995&dest_type=hotel&dist=0&group_adults=2&group_children=0&hapos=1&hpos=1&no_rooms=1&req_adults=2&req_children=0&room1=A%2CA&sb_price_type=total&sr_order=popularity&srepoch=1784947946&srpvid=b3c7143251910627&type=total&ucfs=1& в тенге Используй эту функцию всегда, когда гость спрашивает цену проживания или стоимость номера.",

                    // parameters: {
                    //     type: "object",

                    //     properties: {
                    //     check_in: {
                    //         type: "string",
                    //         description:
                    //         "Дата заезда YYYY-MM-DD",
                    //     },

                    //     check_out: {
                    //         type: "string",
                    //         description:
                    //         "Дата выезда YYYY-MM-DD",
                    //     },

                    //     adults: {
                    //         type: "integer",
                    //         minimum: 1,
                    //         description:
                    //         "Количество взрослых гостей",
                    //     },
                    //     },

                    //     required: [
                    //     "check_in",
                    //     "check_out",
                    //     "adults",
                    //     ],

                    //     additionalProperties: false,
                    // },
                    // }
                ],

            tool_choice: "auto",


                audio: {
                  input: {
                    format: {
                      type:
                        "audio/pcm",

                      rate:
                        24000,
                    },

                    turn_detection: {
                      type:
                        "server_vad",

                      threshold:
                        0.5,

                      prefix_padding_ms:
                        300,

                      silence_duration_ms:
                        550,

                      create_response:
                        true,

                      interrupt_response:
                        true,
                    },
                  },

                  output: {
                    format: {
                      type:
                        "audio/pcm",

                      rate:
                        24000,
                    },

                    voice:
                      OPENAI_VOICE,
                  },
                },
              },
            });
          }
        );

        openAI.on(
          "message",
          async raw => {
            let event;

            try {
              event =
                JSON.parse(
                  raw.toString()
                );
            } catch {
              return;
            }

            if (
              event.type ===
              "session.updated"
            ) {
              if (!ready) {
                ready = true;

                console.log(
                  "✅ OpenAI session готова"
                );

                resolve();
              }

              return;
            }

            if (
              event.type ===
              "input_audio_buffer.speech_started"
            ) {
              console.log(
                "🗣️ Гость начал говорить"
              );

              /*
               * Barge-in.
               *
               * Удаляем ещё не сыгранный
               * ответ бота.
               */
              audioPacer.clear();

              return;
            }

            if (
              event.type ===
              "input_audio_buffer.speech_stopped"
            ) {
              console.log(
                "🤐 Конец фразы гостя"
              );
              return;
            }

if (
  event.type ===
    "response.function_call_arguments.done" &&
  event.name ===
    "transfer_to_department"
) {
  console.log("");
  console.log(
    "🔧 ============================="
  );
  console.log(
    "🔧 OPENAI TOOL CALL"
  );
  console.log(
    `🔧 name: ${event.name}`
  );
  console.log(
    `🔧 args: ${event.arguments}`
  );
  console.log(
    "🔧 ============================="
  );

  try {
    const args =
      JSON.parse(
        event.arguments || "{}"
      );

    const department =
      args.department;

    const destination =
      DEPARTMENTS[department];

    console.log(
      `🏨 Отдел: ${department}`
    );

    console.log(
      `☎️ Destination: ${destination}`
    );

    if (!destination) {
      throw new Error(
        `Для отдела "${department}" не настроен DN`
      );
    }

    /*
     * Убираем голос,
     * который ещё не успел
     * проиграться.
     */
    audioPacer.clear();

    console.log(
      `🔀 TRANSFER ${participantId} → ${destination}`
    );

    const result =
      await transferCall(
        participantId,
        destination
      );

    console.log(
      "✅ TRANSFER RESULT:",
      result
    );

    /*
     * Сообщаем OpenAI,
     * что функция выполнена.
     */
    send({
      type:
        "conversation.item.create",

      item: {
        type:
          "function_call_output",

        call_id:
          event.call_id,

        output:
          JSON.stringify({
            success: true,
            department,
            destination,
          }),
      },
    });

  } catch (error) {
    console.error(
      "❌ TRANSFER ERROR:",
      error.message
    );

    send({
      type:
        "conversation.item.create",

      item: {
        type:
          "function_call_output",

        call_id:
          event.call_id,

        output:
          JSON.stringify({
            success: false,
            error:
              error.message,
          }),
      },
    });

    send({
      type:
        "response.create",

      response: {
        output_modalities: [
          "audio",
        ],

        instructions:
          "Сообщи гостю, что сейчас не удалось перевести звонок, и предложи продолжить разговор.",
      },
    });
  }

  return;
}


            if (
              event.type ===
              "response.output_audio.delta"
            ) {
              const pcm24 =
                Buffer.from(
                  event.delta,
                  "base64"
                );

              const pcm8 =
                downsampler.process(
                  pcm24
                );

              audioPacer.push(
                pcm8
              );

              return;
            }

            if (
              event.type ===
              "response.output_audio_transcript.done"
            ) {
              console.log(
                `🤖 OpenAI: ${event.transcript}`
              );

              return;
            }

            if (
              event.type ===
              "error"
            ) {
              console.error(
                "❌ OpenAI:",
                event.error
              );
            }
          }
        );

        openAI.on(
          "error",
          error => {
            console.error(
              "❌ OpenAI WS:",
              error.message
            );

            if (!ready) {
              reject(error);
            }
          }
        );
      }
    );

  return {
    socket:
      openAI,

    ready:
      () => ready,

    readyPromise,

    send,
  };
}


async function pipe3CXToOpenAI(
  response,
  openAI,
  signal
) {
  const upsampler =
    new Pcm8kTo24k();

  const stream =
    Readable.fromWeb(
      response.body
    );

  for await (
    const chunk of stream
  ) {
    if (signal.aborted) {
      break;
    }

    if (!openAI.ready()) {
      continue;
    }

    const pcm24 =
      upsampler.process(
        chunk
      );

    if (!pcm24.length) {
      continue;
    }

    openAI.send({
      type:
        "input_audio_buffer.append",

      audio:
        pcm24.toString(
          "base64"
        ),
    });
  }
}

const activeBots =  new Map();


async function startVoiceBot(
  participant
) {
  const participantId =
    Number(
      participant.id
    );

  if (
    activeBots.has(
      participantId
    )
  ) {
    return;
  }

  const inputAbort =
    new AbortController();

  const outputAbort =
    new AbortController();

  const session = {
    participantId,
    inputAbort,
    outputAbort,
    openAI: null,
    outputStream: null,
    audioPacer: null,
  };

  activeBots.set(
    participantId,
    session
  );

  try {
    console.log("");
    console.log(
      "🤖 ========================="
    );

    console.log(
      `🤖 VOICE BOT ${participantId}`
    );

    console.log(
      `☎️ ${
        participant.party_caller_id ||
        participant.party_dn ||
        "unknown"
      }`
    );

    console.log(
      "🤖 ========================="
    );

    /*
     * Сначала открываем
     * incoming PCM.
     *
     * Если будет 424 —
     * ждём несколько миллисекунд.
     */

    const incoming =
      await get3CXAudioStream(
        participantId,
        inputAbort.signal
      );

    /*
     * Создаём постоянный
     * POST stream обратно в 3CX.
     */

    const output =
      new PassThrough({
        highWaterMark:
          64 * 1024,
      });

    session.outputStream =
      output;

    const pacer =
      new AudioPacer(
        output
      );

    session.audioPacer =
      pacer;

    /*
     * Чуть тишины в начале,
     * чтобы POST реально стартовал.
     */

    pacer.push(
      Buffer.alloc(320)
    );

    sendAudioTo3CX(
      participantId,
      output,
      outputAbort.signal
    ).catch(error => {
      console.error(
        "❌ Отправка голоса в 3CX:",
        error.message
      );

      stopVoiceBot(
        participantId
      );
    });

    /*
     * Подключаем OpenAI.
     */

    const openAI =
      connectOpenAI(
        participantId,
        pacer
      );

    session.openAI =
      openAI;

    await openAI.readyPromise;

    /*
     * Бот говорит первым.
     */

    openAI.send({
      type:
        "response.create",

      response: {
        output_modalities: [
          "audio",
        ],

        instructions:
          process.env.OPENAI_GREETING,
      },
    });

    /*
     * Голос звонящего:
     *
     * 3CX
     * ↓
     * PCM8k
     * ↓
     * PCM24k
     * ↓
     * OpenAI
     */

    await pipe3CXToOpenAI(
      incoming,
      openAI,
      inputAbort.signal
    );

  } catch (error) {
    console.error(
      "❌ Voice Bot:",
      error.message
    );

    stopVoiceBot(
      participantId
    );
  }
}


function stopVoiceBot(
  participantId
) {
  const id =
    Number(
      participantId
    );

  const session =
    activeBots.get(id);

  if (!session) {
    return;
  }

  console.log(
    `🛑 Останавливаем Voice Bot ${id}`
  );

  session.inputAbort.abort();
  session.outputAbort.abort();

  session.audioPacer?.stop();

  if (
    session.openAI
      ?.socket
      ?.readyState ===
    WebSocket.OPEN
  ) {
    session.openAI
      .socket
      .close();
  }

  activeBots.delete(id);
}
// ======================================================
// ВЫВОД СОСТОЯНИЯ ЗВОНКА
// ======================================================

function printRoutePoint(routePoint) {
  console.log("");
  console.log("==========================================");
  console.log(`📡 ROUTE POINT ${routePoint.dn}`);
  console.log(`📌 Type: ${routePoint.type}`);

  const participants =
    routePoint.participants || [];

  console.log(
    `👥 Participants: ${participants.length}`
  );

  if (participants.length === 0) {
    console.log("☎️ Сейчас активных звонков нет");
  }

  for (const participant of participants) {
    console.log("");
    console.log("📞 УЧАСТНИК ЗВОНКА");

    console.dir(
      {
        id: participant.id,

        status:
          participant.status,

        party_dn:
          participant.party_dn,

        party_dn_type:
          participant.party_dn_type,

        party_caller_id:
          participant.party_caller_id,

        party_caller_name:
          participant.party_caller_name,

        device_id:
          participant.device_id,

        direct_control:
          participant.direct_control,

        callid:
          participant.callid,

        legid:
          participant.legid,
      },
      {
        depth: null,
        colors: true,
      }
    );
  }

  console.log("==========================================");
  console.log("");
}

// ======================================================
// ОБНОВЛЯЕМ СОСТОЯНИЕ 900
// ======================================================

async function refreshRoutePoint() {
  try {
    const token = await getAccessToken();

    const routePoint = await getRoutePoint(token);

for (
  const participant of
  routePoint.participants || []
) {
  const status =
    String(
      participant.status
    ).toLowerCase();

  /*
   * ЗВОНОК ЗВОНИТ
   */

  if (
    status === "ringing"
  ) {
    const answered =
      await answerCall(
        participant
      );

    if (answered) {
      /*
       * Не стартуем audio
       * моментально.
       *
       * Ждём WS update:
       * Ringing → Connected.
       */
      console.log(
        "⏳ Ждём Connected..."
      );
    }
  }

  /*
   * ЗВОНОК УЖЕ ПРИНЯТ
   */

  if (
    status === "connected" || status === "talking"
  ) {
    startVoiceBot(participant);
  }
}

    printRoutePoint(routePoint);
  } catch (error) {
    console.error(
      "❌ Ошибка обновления Route Point:",
      error.message
    );
  }
}

// ======================================================
// WEBSOCKET
// ======================================================

let socket = null;
let reconnectTimer = null;
let pingTimer = null;
let shuttingDown = false;

function getWebSocketUrl() {
  const url = new URL(PBX_URL);

  const protocol =
    url.protocol === "https:"
      ? "wss:"
      : "ws:";

  return `${protocol}//${url.host}/callcontrol/ws`;
}

// ======================================================
// ОБРАБОТКА WS EVENT
// ======================================================

async function handleWebSocketMessage(buffer) {
  const raw = buffer.toString("utf8");

  let message;

  try {
    message = JSON.parse(raw);
  } catch {
    console.log("📨 WS RAW:", raw);
    return;
  }

  console.log("");
  console.log("⚡ НОВОЕ СОБЫТИЕ 3CX");

  console.dir(message, {
    depth: null,
    colors: true,
  });

  /*
   * Обычно событие выглядит примерно так:
   *
   * {
   *   event: {
   *     event_type: 0,
   *     entity: "/callcontrol/900/participants/12345"
   *   }
   * }
   *
   * Но делаем поддержку и варианта,
   * когда event находится сразу в корне.
   */

  const event =
    message.event || message;

  const entity =
    event?.entity;

  const eventType =
    event?.event_type;

  console.log(
    `📌 Event type: ${eventType ?? "unknown"}`
  );

  console.log(
    `🔗 Entity: ${entity ?? "unknown"}`
  );

  /*
   * Нас интересуют только изменения,
   * относящиеся к Route Point 900.
   *
   * Например:
   *
   * /callcontrol/900
   * /callcontrol/900/participants/15667
   */

  if (
    entity &&
    entity.startsWith(`/callcontrol/${DN}`)
  ) {
    console.log(
      `✅ Событие относится к DN ${DN}`
    );

    await refreshRoutePoint();
  } else {
    console.log(
      `⏭ Событие не относится к DN ${DN}`
    );
  }
}

// ======================================================
// RECONNECT
// ======================================================

function scheduleReconnect() {
  if (shuttingDown) {
    return;
  }

  if (reconnectTimer) {
    return;
  }

  console.log(
    "🔄 Переподключаемся к 3CX через 5 секунд..."
  );

  reconnectTimer = setTimeout(
    async () => {
      reconnectTimer = null;

      try {
        await connectWebSocket();
      } catch (error) {
        console.error(
          "❌ Ошибка переподключения:",
          error.message
        );

        scheduleReconnect();
      }
    },
    5000
  );
}

// ======================================================
// CONNECT WS
// ======================================================

async function connectWebSocket() {
  /*
   * При новом WS соединении
   * берём свежий токен.
   */

  const token =
    await getAccessToken(true);

  const wsUrl =
    getWebSocketUrl();

  console.log("");
  console.log("🔌 Подключаем WebSocket");
  console.log(`🌐 ${wsUrl}`);

  socket = new WebSocket(
    wsUrl,
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );

  // ------------------------------------------
  // OPEN
  // ------------------------------------------

  socket.on("open", async () => {
    console.log("");
    console.log(
      "✅ WebSocket подключен к 3CX"
    );

    console.log(
      `👀 Слушаем изменения DN ${DN}`
    );

    /*
     * Отдельное сообщение subscribe
     * здесь не отправляем.
     */

    clearInterval(pingTimer);

    pingTimer = setInterval(() => {
      if (
        socket?.readyState ===
        WebSocket.OPEN
      ) {
        socket.ping();
      }
    }, 15_000);

    // Показываем текущее состояние
    await refreshRoutePoint();
  });

  // ------------------------------------------
  // MESSAGE
  // ------------------------------------------

  socket.on(
    "message",
    async (data) => {
      try {
        await handleWebSocketMessage(data);
      } catch (error) {
        console.error(
          "❌ Ошибка обработки WS:",
          error.message
        );
      }
    }
  );

  // ------------------------------------------
  // ERROR
  // ------------------------------------------

  socket.on(
    "error",
    (error) => {
      console.error(
        "❌ WebSocket error:",
        error.message
      );
    }
  );

  // ------------------------------------------
  // CLOSE
  // ------------------------------------------

  socket.on(
    "close",
    (code, reason) => {
      console.log("");
      console.log(
        `🔌 WebSocket закрыт`
      );

      console.log(
        `Code: ${code}`
      );

      if (reason?.length) {
        console.log(
          `Reason: ${reason.toString()}`
        );
      }

      clearInterval(pingTimer);
      pingTimer = null;

      scheduleReconnect();
    }
  );
}

// ======================================================
// START
// ======================================================

async function start() {
  try {
    console.log(
      "🚀 3CX сервер запускается"
    );

    console.log(
      `📡 PBX: ${PBX_URL}`
    );

    console.log(
      `📞 Route Point: ${DN}`
    );

    // 1. Получаем токен
    const token =
      await getAccessToken();

    // 2. Проверяем REST
    const routePoint =
      await getRoutePoint(token);

    console.log(
      "\n✅ Соединение с 3CX работает\n"
    );

    printRoutePoint(routePoint);

    // 3. Подключаем Real-Time WS
    await connectWebSocket();

  } catch (error) {
    console.error("\n❌ Ошибка:");
    console.error(error.message);

    scheduleReconnect();
  }
}

start();

// ======================================================
// CTRL+C
// ======================================================

process.on("SIGINT", () => {
  shuttingDown = true;

  console.log(
    "\n🛑 Останавливаем сервер..."
  );

  clearTimeout(
    reconnectTimer
  );

  clearInterval(
    pingTimer
  );

  if (socket) {
    socket.close();
  }

  process.exit(0);
});
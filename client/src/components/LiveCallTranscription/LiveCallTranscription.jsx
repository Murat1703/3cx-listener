import {
  useEffect,
  useState,
} from "react";

const WS_URL =
  import.meta.env
    .VITE_CALLS_WS_URL ||
  "ws://localhost:8081";

function getMessageKey(message) {
  return String(
    message.callId ??
      message.call?.callId ??
      message.entity ??
      message.call?.entity ??
      "unknown"
  );
}

export const LiveCallTranscription = () =>{
  const [isConnected, setIsConnected] =
    useState(false);

  const [calls, setCalls] =
    useState({});

  useEffect(() => {
    let socket;
    let reconnectTimer;
    let destroyed = false;

    const connect = () => {
      socket = new WebSocket(WS_URL);

      socket.onopen = () => {
        console.log(
          "✅ Подключились к серверу звонков"
        );

        setIsConnected(true);
      };

      socket.onmessage = (event) => {
        let message;

        try {
          message = JSON.parse(
            event.data
          );
        } catch {
          return;
        }

        if (
          message.type ===
          "system.connected"
        ) {
          return;
        }

        const key =
          getMessageKey(message);

        setCalls((previousCalls) => {
          const current =
            previousCalls[key] || {
              callId:
                message.callId ||
                message.call?.callId ||
                null,

              from:
                message.call?.from ||
                null,

              to:
                message.call?.to ||
                null,

              status: "unknown",

              completedLines: [],
              partials: {},
            };

          if (
            message.type ===
            "call.updated"
          ) {
            return {
              ...previousCalls,

              [key]: {
                ...current,
                ...message.call,
              },
            };
          }

          if (
            message.type ===
            "call.finished"
          ) {
            return {
              ...previousCalls,

              [key]: {
                ...current,
                ...message.call,
                status: "finished",
              },
            };
          }

          if (
            message.type ===
            "transcript.delta"
          ) {
            const itemId =
              message.itemId ||
              "current";

            return {
              ...previousCalls,

              [key]: {
                ...current,

                partials: {
                  ...current.partials,

                  [itemId]:
                    (
                      current.partials[
                        itemId
                      ] || ""
                    ) + message.delta,
                },
              },
            };
          }

          if (
            message.type ===
            "transcript.completed"
          ) {
            const partials = {
              ...current.partials,
            };

            delete partials[
              message.itemId
            ];

            return {
              ...previousCalls,

              [key]: {
                ...current,

                partials,

                completedLines: [
                  ...current.completedLines,

                  {
                    id:
                      message.itemId ||
                      crypto.randomUUID(),

                    text:
                      message.transcript,

                    createdAt:
                      new Date()
                        .toISOString(),
                  },
                ],
              },
            };
          }

          if (
            message.type ===
            "transcription.error"
          ) {
            return {
              ...previousCalls,

              [key]: {
                ...current,
                error: message.message,
              },
            };
          }

          return previousCalls;
        });
      };

      socket.onerror = (error) => {
        console.error(
          "WebSocket error:",
          error
        );
      };

      socket.onclose = () => {
        setIsConnected(false);

        if (!destroyed) {
          reconnectTimer =
            setTimeout(
              connect,
              3000
            );
        }
      };
    };

    connect();

    return () => {
      destroyed = true;

      clearTimeout(reconnectTimer);
      socket?.close();
    };
  }, []);

  const callList =
    Object.entries(calls);

  return (
    <section>
      <h2>
        Распознавание звонков
      </h2>

      <p>
        Сервер:{" "}
        {isConnected
          ? "подключён"
          : "отключён"}
      </p>

      {callList.length === 0 && (
        <p>
          Активных звонков пока нет.
        </p>
      )}

      {callList.map(
        ([key, call]) => (
          <article
            key={key}
            style={{
              marginBottom: 20,
              padding: 16,
              border:
                "1px solid #444",
              borderRadius: 12,
            }}
          >
            <h3>
              {call.direction ===
              "outgoing"
                ? "Исходящий звонок"
                : "Входящий звонок"}
            </h3>

            <p>
              От:{" "}
              {call.from ||
                "не определено"}
            </p>

            <p>
              Кому:{" "}
              {call.to ||
                "не определено"}
            </p>

            <p>
              Статус:{" "}
              {call.status}
            </p>

            {call.error && (
              <p>
                Ошибка: {call.error}
              </p>
            )}

            <div
              style={{
                whiteSpace:
                  "pre-wrap",
                lineHeight: 1.6,
              }}
            >
              {call.completedLines.map(
                (line) => (
                  <p key={line.id}>
                    {line.text}
                  </p>
                )
              )}

              {Object.entries(
                call.partials
              ).map(
                ([itemId, text]) => (
                  <p
                    key={itemId}
                    style={{
                      opacity: 0.6,
                    }}
                  >
                    {text}
                  </p>
                )
              )}
            </div>
          </article>
        )
      )}
    </section>
  );
}
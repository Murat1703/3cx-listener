# Sadu 3CX + OpenAI voice bot

Готовый Node.js-сервис для:

- получения событий 3CX Call Control через WebSocket;
- чтения и отправки PCM16 8000 Hz mono через participant stream;
- преобразования 8 kHz ↔ 24 kHz через FFmpeg;
- голосового диалога через OpenAI Realtime;
- отправки расшифровки/состояния звонка на React через WebSocket;
- отправки итогового письма через SMTP;
- запуска бота только для входящих звонков и разрешённых тестовых номеров.

## Главное исправление для 424

`THREECX_EXTENSION` должен указывать на DN **Route Point / API application**, которому принадлежит звонок. Если поставить обычный внутренний номер, события звонка могут приходить, но GET/POST `/stream` часто возвращает `424 Failed Dependency`.

В 3CX Admin Console:

1. Открой `Integrations → API`.
2. Создай приложение Call Control.
3. Включи `3CX Call Control API Access`.
4. Укажи Route Point DN, например `005`.
5. Направь тестовый маршрут/DID на этот Route Point.
6. Для безопасного теста укажи внешний номер в `ALLOWED_CALLER_NUMBERS`.

## Установка

```bash
cp .env.example .env
npm install
npm run check
npm run health
npm run test:mock
npm start
```

Требования: Node.js 20+ и `ffmpeg` в PATH.

macOS:

```bash
brew install ffmpeg
```

Ubuntu/Debian:

```bash
sudo apt update
sudo apt install -y ffmpeg
```

## Ограничение одним тестовым номером

```env
VOICE_BOT_INCOMING_ONLY=true
ALLOWED_CALLER_NUMBERS=77052941444
```

Допускаются форматы `7052941444`, `87052941444` и `+77052941444` — они приводятся к одному виду.

## React WebSocket

По умолчанию сервис слушает:

```text
ws://127.0.0.1:8081
```

Для продакшена лучше проксировать WebSocket через Nginx, не открывая порт 8081 наружу.

## Что исправлено

- устранён `ReferenceError: Cannot access 'openAiWs' before initialization`;
- путь импорта исправлен на `./live-transcription.js`;
- стартовая проверка выполняется по конкретному DN `/callcontrol/005`, а не по глобальному endpoint;
- повторное получение 3CX-токена добавлено для GET/POST audio stream после `401`;
- увеличены задержка и число повторов при `404/424`;
- добавлена проверка наличия FFmpeg;
- добавлен таймаут подключения OpenAI WebSocket;
- добавлена фильтрация входящих звонков по allowlist;
- frontend WebSocket по умолчанию привязан к localhost;
- подробный лог всех OpenAI events выключен по умолчанию;
- добавлены low-latency параметры FFmpeg и ожидание подтверждения `session.updated`;
- входной звук не отправляется до завершения настройки OpenAI-сессии;
- при перебивании ответа очищается очередь и перезапускается выходной ресемплер.

## Если остаётся 403

Проверь, что API-приложение имеет Call Control scope и доступ именно к DN из `THREECX_EXTENSION`. Команда `npm run health` покажет, на каком этапе возникает запрет.

## Если остаётся 424

Проверь по порядку:

1. DN является Route Point приложения, а не обычным extension.
2. Participant уже в статусе `connected`.
3. Звонок действительно принадлежит этому Route Point.
4. В 3CX используется редакция/лицензия с Call Control API.
5. Нет второго процесса, одновременно открывающего stream того же participant.

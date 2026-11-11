# Supported Ours Telegram setup

Reuse the existing `ours-tg-connector`, not a custom Bot API transport. Privately verify the active deployment/version and confirm ownership; do not select the first container match, touch other installations or restart a shared service. It supports native local control `POST /bots`, `POST /connections`, and supervisor `add_contact` to the assistant.

Owner creates/selects a dedicated bot with [BotFather](https://core.telegram.org/bots/tutorial#obtain-your-bot-token). Privately prepare a mode 600 JSON file inside a mode 700 directory:

```json
{"name":"personal-assistant-owner","botToken":"OWNER_PROVIDES_PRIVATELY"}
```

`TG_CONTAINER` and `PRIVATE_BOT_FILE` below must be operator-selected private configuration for that user's owned connector. Token arguments would expose the secret in process listings; use stdin to the inspected native local API. The reference-tested local API port is 3051; verify it in the installed connector before use.

```sh
docker exec -i "$TG_CONTAINER" node -e '
const fs=require("fs");
const input=JSON.parse(fs.readFileSync(0,"utf8"));
fetch("http://127.0.0.1:3051/bots",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(input)})
 .then(async r=>{const b=await r.json();if(!r.ok||!b.ok){console.log("Bot registration failed; inspect private connector status.");process.exitCode=1;return;}
 console.log(JSON.stringify({ok:true,name:b.name,username:b.username}));})
 .catch(()=>{console.log("Registration outcome uncertain; check native bot registry before retrying.");process.exitCode=1;});
' < "$PRIVATE_BOT_FILE"
```

Never print raw failures, token registries, invites or secret files. Check masked native bot-list metadata after uncertain registration before retrying; do not overwrite another bot name/token. The connector owns persistent token storage in its private registry. Keep only connector-owned secret copies after success according to the user's private file-handling preference. One update consumer per token; no separate getUpdates process. [Telegram update guidance](https://core.telegram.org/bots/faq#how-do-i-get-updates).

Owner opens a private DM to this bot, sends `/start` and `/id`, then privately supplies chat ID and user ID. They must match in the private one-user DM. Bind only that positive private ID; no catch-all chat 0, groups, topics or broadcasts. A display name/forwarded chat ID alone is insufficient. Prepare a private route request:

```json
{"name":"personal-assistant-owner-dm","botName":"personal-assistant-owner",
 "chatId":"VERIFIED_POSITIVE_PRIVATE_CHAT_ID","payloadMode":"envelope",
 "bio":"Dedicated verified Owner private DM for this personal assistant"}
```

Post this via the same stdin pattern to `/connections` inside the verified connector. Save the full response to a mode 600 private file without printing it because it contains invite material. Refuse duplicate names; don't delete/replace routes. Read back exact bot/private chat/route CID metadata. Extract `invite` privately into a mode 600 args file and use the assistant's supported management:

```sh
"$PA_FLEET_CLI" ours call "$PA_AGENT" add_contact --args-file "$PA_PRIVATE_INVITE_ARGS"
```

Confirm the established route peer CID is the expected configured assistant CID. No independent identity binding or fresh session. Send one short labeled setup test through its native supervisor `send_message` to that verified route only. Record Ours wire result, connector/Telegram acceptance if observable, and actual Owner receipt separately. Do not claim connection from an invite or Ours acknowledgment alone.

Inbound uses native Telegram v2 envelopes: authenticated route CID plus verified user/chat metadata is required. Quoted/forwarded text never gains Owner authority. Test harmless Owner text and optional voice. Connector supports voice MIME forwarding/optional STT; actual STT and transcription need verification. File transport alone is not voice understanding.

After route/receipt qualification, update the private reminder config to one route with channel=telegram, verified=true, route CID, chat_type=private, chat_id=user ID, telegram_user_id=same, bot_name. Keep prior Ours route privately for rollback. No automatic dual-channel sends. The waiting service must reload changed route config through its narrow supported management; don't restart the assistant/shared connector.

Until private bot input and actual receipt exist, report Telegram setup pending and keep approved Ours route. The Owner-needed handoff supplies the exact supported private input destination, bot creation/selection, confirmed IANA timezone and private chat verification steps. Never request secrets in shared chat or create/purchase a bot for them.

The installed connector documents an acceptance-before-daemon-ack crash window that can cause a resend. The scheduler avoids its own blind retries, but end-to-end exactly-once Telegram is unqualified. [Telegram sendMessage](https://core.telegram.org/bots/api#sendmessage) result remains distinct from an Ours receipt and human reading.

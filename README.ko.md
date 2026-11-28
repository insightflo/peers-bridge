# hermes-peers-bridge

[English](README.md) | **한국어**

**[Hermes Agent](https://hermes-agent.nousresearch.com)를 위한 네트워크 간 피어 메시징 — 서로 다른 머신의 AI 에이전트들이 대화할 수 있게 합니다.**

여러 대의 Hermes 인스턴스(서로 다른 머신·네트워크)를 중앙 [claude-peers](https://github.com/louislva/claude-peers-mcp) 브로커 허브로 연결하는 Hermes Agent 플러그인입니다. 각 에이전트는 이름을 가진 피어로 등록되고, 별명으로 서로에게 메시지를 보낼 수 있습니다 — 클라우드 VM의 게이트웨이 에이전트가 노트북의 에이전트에게 로컬 작업을 요청하고, 그 답을 일반 대화 턴으로 받아볼 수 있습니다.

```
┌─────────┐      ┌──────────────────┐      ┌─────────┐
│ Agent A │◄────►│   Remote Hub     │◄────►│ Agent B │
│ (노트북) │      │ (브로커, HTTPS)   │      │  (VM)   │
└─────────┘      └──────────────────┘      └─────────┘
                        ▲
                        │
                  ┌─────────┐
                  │ Agent C │
                  │ (서버)   │
                  └─────────┘
```

## 누구를 위한 플러그인인가요?

- **여러 머신에서 Hermes Agent를 운영**하는 경우 (예: 노트북 + 홈서버 + 클라우드 VM) — 에이전트끼리 작업 위임, 질문, 상태 보고 등 협업이 필요할 때.
- 각 머신을 외부에 노출하지 않고 **네트워크를 넘어 에이전트 간 메시징**을 하고 싶을 때 — 허브 한 대만 공개 엔드포인트가 있으면 됩니다.
- 같은 머신 안에서 로컬 `claude-peers` 브로커를 쓰고 있고, 이를 여러 호스트로 확장하고 싶을 때.

요구 사항: 플러그인을 지원하는 Hermes Agent, Python 3.10+, 그리고 브로커를 돌릴 호스트 1대 (Bun/Node + 공개 HTTPS 엔드포인트 — 리버스 프록시나 Cloudflare Tunnel 뒤에 두면 됩니다).

## 주요 기능

- **이름 있는 피어** — 불투명한 ID 대신 사람이 읽을 수 있는 별명(`Laptop`, `HomeServer`, `CloudVM`)으로 식별하고, 별명 또는 ID로 전송.
- **게이트웨이/CLI 자동 구분** — 게이트웨이 프로세스는 원격 허브에, 대화형 CLI/TUI 세션은 로컬 브로커에 등록. 서브에이전트는 자동 제외.
- **실제 메시지 전달** — 수신한 피어 메시지를 게이트웨이 홈채널 세션에 일반 에이전트 턴으로 주입하므로, 받는 쪽 에이전트가 실제로 읽고 답장합니다 (최신·구버전 Hermes 게이트웨이 빌드 모두 지원).
- **중복 방지** — 메시지 ID 기반 중복 제거와 허브 방식에 맞춘 전달 처리로 같은 메시지가 두 번 처리되지 않습니다.
- **자가 복구 등록** — 하트비트 기반 생존 확인, 브로커 재시작·일시 장애 후 자동 재등록.
- **OpenBot 머신** — 머신당 브리지 데몬 1개가 해당 머신의 OpenBot 에이전트 목록을 허브에 보고합니다. 에이전트는 대시보드에서 자식 피어로 등록하고, 다른 피어와 같은 방식으로 별명으로 호출합니다.
- **첨부 파일** — 메시지당 최대 5개 파일(각 20 MB)을 첨부할 수 있고, 받는 쪽은 파일을 로컬로 내려받아 절대 경로 형태로 에이전트에게 전달합니다.

## 설치 방법

### 1. 허브 브로커 설치 (머신 1대)

업스트림 `claude-peers` 브로커는 모든 피어가 브로커 호스트에 있다고 가정합니다(PID를 로컬에서 확인). 네트워크 간 허브로 쓰려면 이 저장소의 패치판 `broker.remote-hub.ts`를 사용하세요 — PID 대신 하트비트 나이로 생존을 판단합니다:

```bash
git clone https://github.com/louislva/claude-peers-mcp
cd claude-peers-mcp
# 기본 브로커를 원격 허브 버전으로 교체
curl -sL https://raw.githubusercontent.com/insightflo/hermes-peers-bridge/main/broker.remote-hub.ts \
  -o broker.ts
```

환경 변수를 설정하고 서비스로 실행합니다 (systemd 등):

```bash
CLAUDE_PEERS_PORT=7899
CLAUDE_PEERS_DB=/var/lib/claude-peers/peers.db
CLAUDE_PEERS_HUB_TOKEN=<길고-무작위인-토큰-생성>
# 선택 사항; 이 시간(ms)보다 오래된 피어는 정리됩니다. 기본 90000.
CLAUDE_PEERS_STALE_TIMEOUT_MS=90000
```

7899 포트를 HTTPS로 노출한 뒤(리버스 프록시 / Cloudflare Tunnel) 확인:

```bash
curl -H "Authorization: Bearer $CLAUDE_PEERS_HUB_TOKEN" \
  https://your-broker.example.com/health
# → {"status":"ok","peers":0}
```

> 허브 토큰은 비밀로 유지하세요. `chmod 600` 권한의 env 파일에 저장하고, 절대 커밋하지 마세요.

### 2. 플러그인 설치 (모든 Hermes 머신)

플러그인 파일을 Hermes 플러그인 디렉토리에 복사합니다:

```bash
mkdir -p ~/.hermes/plugins/claude_peers_bridge
cd ~/.hermes/plugins/claude_peers_bridge
for f in __init__.py bridge.py schemas.py plugin.yaml; do
  curl -sLO https://raw.githubusercontent.com/insightflo/hermes-peers-bridge/main/$f
done
python3 -m py_compile bridge.py && echo OK
```

`~/.hermes/config.yaml`에서 활성화:

```yaml
plugins:
  enabled:
    - claude-peers-bridge
```

`~/.hermes/.env` 설정:

```bash
# 원격 허브 (게이트웨이 프로세스가 사용)
CLAUDE_PEERS_BROKER_URL=https://your-broker.example.com
CLAUDE_PEERS_BROKER_AUTH=Bearer <허브-토큰>
# 이 머신 피어의 고유한, 사람이 읽을 수 있는 이름
CLAUDE_PEERS_NAME=Laptop
```

### 3. 메시지 수신 배선 (게이트웨이 머신)

받는 쪽 에이전트가 피어 메시지를 실제로 **읽고 답장**할 수 있는지는 두 가지 설정에 달려 있습니다:

**a) 홈채널** — 수신한 피어 메시지는 게이트웨이의 홈채널 세션에 주입됩니다. 게이트웨이가 사용하는 플랫폼의 홈채널을 `~/.hermes/.env`에 설정하세요:

```bash
TELEGRAM_HOME_CHANNEL=<chat-id>     # 게이트웨이가 Telegram을 쓰는 경우
# 또는
SLACK_HOME_CHANNEL=<dm-channel-id>  # 게이트웨이가 Slack을 쓰는 경우
```

**b) 툴셋** — 받는 쪽 에이전트가 답장하려면 `claude-peers` 도구가 필요합니다. `~/.hermes/config.yaml`에서 해당 플랫폼의 툴셋 목록에 `claude-peers`를 추가하세요:

```yaml
platform_toolsets:
  slack:            # 또는 telegram, discord, ...
    - claude-peers
    # ...기존 항목들...
```

(b)가 없으면 에이전트가 메시지를 받아도 조용히 답장에 실패합니다 — 그 세션에 전송 도구가 없기 때문입니다.

### 4. 재시작 및 검증

```bash
hermes gateway restart          # 또는: systemctl --user restart hermes-gateway
```

아무 에이전트에서나 피어 목록을 확인하고 왕복 테스트를 보냅니다:

```text
claude_peers_list_peers()
claude_peers_send_message(to_id="CloudVM", message="ping — ACK 부탁해")
```

원격 에이전트가 홈채널 세션에서 메시지를 받고, 다음 폴링 주기(수 초) 안에 답장해야 정상입니다.

### 5. OpenBot 머신 (선택 사항)

OpenBot 머신은 Hermes 플러그인 대신 독립 데몬 `openbot_bridge.py`를 실행합니다 (Python 3만 있으면 되고 의존성 없음, `openbot-cli`용 `node` 필요). **머신당 데몬 1개**만 띄웁니다. 데몬은 OpenBot *호스트*로 등록해 `openbot-cli list --json` 결과의 에이전트 목록을 허브에 보고하고, 나중에 대시보드에서 등록한 에이전트 피어의 작업을 실행합니다.

```bash
git clone https://github.com/insightflo/peers-bridge.git "$HOME/Projects/peers-bridge"
mkdir -p ~/.config/peers-bridge
cat > ~/.config/peers-bridge/openbot-mac.env <<EOF
CLAUDE_PEERS_BROKER_URL=https://your-broker.example.com
CLAUDE_PEERS_BROKER_AUTH=<허브-토큰>
CLAUDE_PEERS_NAME=OpenBot-Mac
OPENBOT_NODE_BIN=$(command -v node)
EOF
chmod 600 ~/.config/peers-bridge/openbot-mac.env
python3 ~/Projects/peers-bridge/openbot_bridge.py
```

데몬은 `~/.config/peers-bridge/openbot-mac.env` → `/etc/openbot-peers-bridge.env` → `PEERS_BRIDGE_ENV_FILE`로 지정한 파일 순으로 읽으며, 처음 찾은 파일만 사용합니다 (이미 설정된 env 변수는 유지).

| 변수 | 용도 |
|------|------|
| `CLAUDE_PEERS_BROKER_URL` | 허브 URL (기본 `https://peers.insightflo.si`) |
| `CLAUDE_PEERS_BROKER_AUTH` | 허브 토큰. 값 전체가 `Authorization` 헤더로 전송되므로 `Bearer `를 포함해 쓰세요 |
| `CLAUDE_PEERS_NAME` | 필수. **호스트 모드**에서는 호스트 별명, 구버전 단일 에이전트 모드에서는 피어 별명 |
| `OPENBOT_AGENT` | 선택. 미설정 → **호스트 모드**. 에이전트 id를 설정하면 모든 메시지가 그 에이전트로 가는 구버전 단일 에이전트 데몬 |
| `OPENBOT_CLI_PATH` | 선택. `openbot-cli.mjs` 경로. 표준 OpenBot.app 설치 경로에서 자동 탐지 |
| `OPENBOT_NODE_BIN` | 선택. node 바이너리 (기본 `node`). 셸 `PATH`가 없는 launchd/systemd에서는 절대 경로로 쓰세요 |
| `OPENBOT_AGENTS_REFRESH_SECONDS` | 선택. 호스트가 `openbot-cli list --json`를 다시 실행하는 주기. 기본 `300` |
| `PEERS_BRIDGE_ATTACHMENT_DIR`, `PEERS_BRIDGE_ATTACHMENT_KEEP_HOURS` | 수신 첨부 파일 캐시 — [첨부 파일](#첨부-파일-파일--이미지) 참고 |

macOS에서 서비스로 실행하려면:

```bash
mkdir -p ~/Library/LaunchAgents ~/Library/Logs
cat > ~/Library/LaunchAgents/com.insightflo.peers-openbot-bridge.plist <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
<key>Label</key><string>com.insightflo.peers-openbot-bridge</string>
<key>ProgramArguments</key>
<array><string>/usr/bin/env</string><string>python3</string><string>$HOME/Projects/peers-bridge/openbot_bridge.py</string></array>
<key>RunAtLoad</key><true/>
<key>KeepAlive</key><true/>
<key>StandardOutPath</key><string>$HOME/Library/Logs/openbot-peers-bridge.out.log</string>
<key>StandardErrorPath</key><string>$HOME/Library/Logs/openbot-peers-bridge.err.log</string>
</dict>
</plist>
EOF
launchctl load ~/Library/LaunchAgents/com.insightflo.peers-openbot-bridge.plist
tail -n 50 -f ~/Library/Logs/openbot-peers-bridge.err.log
```

env 파일을 고친 뒤에는 `launchctl unload <plist> && launchctl load <plist>`로 재적재합니다. (대시보드의 **Install bridge on a machine** 패널에 브로커 URL이 채워진 같은 내용과 Linux용 systemd 사용자 유닛이 있습니다.)

확인: 약 15초 안에 그 머신이 대시보드 Peer sessions에 에이전트 수가 표시된 온라인 호스트로 나타나야 합니다. 그 이후에만 에이전트를 등록할 수 있습니다 (다음 섹션).

### 6. 대시보드에서 OpenBot 에이전트 등록

`https://your-broker.example.com/dashboard` → **+ Register Peer** → Kind **OpenBot agent**. 호스트를 고르고 공개할 에이전트를 체크해 등록하면, 체크한 에이전트마다 `openbot` 피어(역할 `openbot`)가 하나씩 생기고 다른 피어가 별명으로 호출할 수 있습니다.

브로커가 강제하는 선행조건 (HTTP 400, 대시보드에 그대로 표시):

- 호스트가 **온라인**이어야 함 — `host is offline; install and start the bridge on that machine first`
- 에이전트가 호스트가 최근 보고한 목록에 있어야 함 — `agent not found on host`
- 호스트가 OpenBot 호스트여야 함 — `host not found` / `host is not an OpenBot host`

별명 규칙: `<접두사>-<에이전트 이름>`, 접두사 기본값은 호스트 별명입니다. 브로커 별명은 ASCII 영문·숫자·공백·`. _ @ -`(64자 이하)만 허용하므로, 한글처럼 비ASCII 이름은 `<접두사>-<에이전트 id>`로 떨어집니다. 다른 체계를 원하면 등록 전에 접두사를 바꾸세요. (별명은 나중에 **Peer settings**에서 수정할 수 있습니다.)

## 사용법

플러그인이 제공하는 에이전트 도구:

| 도구 | 용도 |
|------|------|
| `claude_peers_list_peers` | 살아있는 피어 목록 (별명, ID, 요약, 마지막 접속) |
| `claude_peers_send_message` | 별명 또는 ID로 피어에게 메시지 전송 |
| `claude_peers_check_messages` | 읽지 않은 메시지 수동 폴링 |
| `claude_peers_set_alias` | 이 피어의 별명 변경 |
| `claude_peers_set_summary` | 이 피어의 "지금 하는 일" 요약 갱신 |
| `claude_peers_bridge_status` | 등록 상태, 브로커 URL, 폴링 상태 확인 |

별명은 1–64자 ASCII (영문·숫자·공백·`.`·`_`·`@`·`-`)이며, 허브 전체에서 대소문자 구분 없이 고유해야 합니다. 수신 메시지에는 `from_alias`와 `from_id`가 모두 담깁니다.

### OpenBot 에이전트

OpenBot 에이전트에게는 위 도구 표가 없습니다. 데몬이 수신 메시지를 프롬프트로 조립해 `openbot-cli run <agent> -`로 실행하고, 프롬프트 끝에는 답장 힌트 `Reply command: peers-send "<from_alias>" "<your reply>" --as "<child alias>"`가 붙습니다. 에이전트는 이 명령을 **나중에 자기 프로세스에서** 실행하므로 데몬의 환경변수가 전달되지 않습니다. 브로커 주소·토큰은 env 파일(`~/.config/peers-bridge/openbot-mac.env`)에서 읽고, 보내는 신원은 `--as`가 지정합니다(호스트 모드에서 자식 별명). `CLAUDE_PEERS_BROKER_AUTH`는 `Bearer <token>` 또는 토큰 값만 써도 됩니다. 같은 스크립트가 CLI 역할도 합니다:

```bash
python3 ~/Projects/peers-bridge/openbot_bridge.py list                          # 허브의 모든 피어 (peers-list도 같은 명령)
python3 ~/Projects/peers-bridge/openbot_bridge.py send <alias> "text"            # CLAUDE_PEERS_NAME으로 전송
python3 ~/Projects/peers-bridge/openbot_bridge.py peers-send <alias> "text" --as "OpenBot-Mac-비서"  # 신원 지정
```

프롬프트는 이 명령을 `peers-send`로 부르며, 스크립트도 `peers-send` / `peers-list`를 서브커맨드 이름으로 받습니다. 에이전트가 `peers-send`를 그대로 실행하게 하려면 PATH가 걸린 래퍼(예: `exec python3 <repo>/openbot_bridge.py peers-send "$@"`)를 두세요. 모든 서브커맨드가 데몬과 같은 env 파일을 읽습니다.

## 첨부 파일 (파일 · 이미지)

메시지에는 **최대 5개, 각 20 MB**의 파일을 첨부할 수 있습니다(브로커 기본값, 아래 env 변수 참고). 흐름은 "업로드 후 참조"입니다. 보내는 쪽이 각 파일을 `/attachments`로 `POST`하고, 돌려받은 id와 함께 메시지를 보냅니다. 받는 쪽 브리지는 파일을 `~/.peers-bridge/attachments/<메시지 id>/<이름>`으로 내려받아 **절대 경로**를 에이전트에 넘기는 프롬프트 끝에 덧붙입니다:

```
Attachments:
- /Users/me/.peers-bridge/attachments/1842/report.pdf (report.pdf, application/pdf, 48213 bytes)
```

**전달되는 것은 경로이지 이미지 자체가 아닙니다.** 에이전트가 이미지를 *볼* 수 있는지는 그 에이전트의 도구와 모델에 달려 있습니다 — 로컬 파일을 읽을 수 있는 Hermes·OpenBot 에이전트는 열 수 있고, 텍스트 전용 에이전트는 못 합니다.

보내는 방법:

- **Hermes 도구** — `claude_peers_send_message(to_id, message, file_paths=["/path/to/file", ...])`. 파일 없음·크기 초과·개수 초과, 업로드 거절은 메시지를 보내지 않고 `{"ok": false, "sent": false, "error": ...}`를 반환합니다.
- **Hermes CLI** — `hermes claude-peers send <to> "<message>" --file <path> [--file <path> ...]` (`-f`도 가능).
- **OpenBot 쪽** — `peers-send <alias> "<text>" --file <path> [--file <path> ...]` (`-f <path>` / `--file=<path>`도 가능). 업로드가 하나라도 실패하면 메시지를 보내지 않고 exit 1로 끝납니다.
- **대시보드** — Send 탭과 피어별 Quick send에 모두 **Attachments** 입력이 있습니다(여러 파일 가능, 메시지 목록에서는 이미지가 썸네일로 표시).

브로커 환경 변수(허브): `CLAUDE_PEERS_ATTACHMENT_MAX_BYTES`(기본 20 MB = `20971520`), `CLAUDE_PEERS_ATTACHMENT_DIR`(기본 `<db 디렉터리>/attachments`), `CLAUDE_PEERS_ATTACHMENT_RETENTION_HOURS`(기본 168), `CLAUDE_PEERS_ATTACHMENT_GC_SECONDS`(기본 300). 허용 MIME: `image/*`, `text/*`, `audio/*`, `video/*`, `application/pdf|json|zip|octet-stream|msword|openxmlformats…`. 그 외는 `File type <mime> is not allowed`로 거절됩니다.

허브 보존 기간: 어떤 메시지도 참조하지 않는 업로드는 24시간 후 삭제, 메시지가 참조하는 파일은 참조 메시지가 모두 보존 기간(7일)을 넘길 때까지 유지합니다. 받는 쪽은 `PEERS_BRIDGE_ATTACHMENT_DIR` 아래에 `PEERS_BRIDGE_ATTACHMENT_KEEP_HOURS`(기본 168, `0`이면 매 폴링 후 삭제) 동안 로컬 사본을 둡니다 — Hermes·OpenBot 양쪽 모두에 적용됩니다.

## 아키텍처 노트

| 프로세스 | 브로커 대상 |
|---------|-----------|
| 게이트웨이 에이전트 (`_HERMES_GATEWAY=1`) | 원격 허브 (`CLAUDE_PEERS_BROKER_URL`) |
| CLI / TUI 에이전트 (`HERMES_INTERACTIVE=1`) | 로컬 브로커 (`127.0.0.1:7899`) |
| 서브에이전트 (kanban / delegate) | 등록 제외 |
| OpenBot 호스트 데몬 (`openbot_bridge.py`, 호스트 모드) | 원격 허브 — `kind: "openbot-host"`로 등록, 자식 피어를 대신해 폴링 |

허브의 **피어 종류(kind)**: `peer`(Hermes 브리지와 대시보드 주소록 항목), `openbot-host`(OpenBot 머신당 1개, 오래되어도 지워지지 않는 managed 항목), `openbot`(호스트 아래 등록된 에이전트, `host_id` + `agent_id`). 자식은 스스로 오래되지 않고 호스트 하트비트를 따라 상태가 바뀌며, 호스트의 하트비트 응답이 자식 목록(`served_peers`)을 실어 주므로 데몬은 재시작 없이 새 등록을 알게 됩니다. 자식에게 온 메시지는 호스트의 SSE 연결을 깨우고, 호스트는 `to_id IN (호스트, 자식…)`으로 폴링해 `to_id`로 라우팅한 뒤 `openbot-cli run <agent_id> -`를 프롬프트에 자식 별명을 넣어 실행합니다. 호스트를 삭제하면 자식과 자식의 미전달 메시지도 삭제됩니다.

**첨부 파일**: 업로드는 `<ATTACHMENT_DIR>/<id>`로 저장되고 `attachments` 테이블(`id`, `name`, `size`, `mime`, `sha256`, `uploaded_by`)에 행이 생깁니다. `messages.attachments`는 참조 JSON 배열이고, 브로커가 모든 조회(`poll-messages`, `/api/messages`, 스냅샷, SSE `message`)에서 메타데이터로 해석해 줍니다. Bun의 `formData()`는 파일명으로 `File.type`을 추론해 MIME 허용 목록을 적용할 수 없게 하므로, 업로드에는 자체 소형 multipart 파서를 씁니다.

- **브로커를 import 시점이 아닌 루프마다 해석** — 플러그인 모듈은 게이트웨이 env가 설정되기 전에 import되므로, 매 주기 재해석해야 게이트웨이가 원격 허브를 자동으로 인식합니다.
- **게이트웨이 주입 + 폴백** — 수신 메시지는 게이트웨이 wake 메커니즘으로 전달되며, `gateway.wake`가 없는 구버전 Hermes에서는 합성 메시지 이벤트 주입으로 폴백합니다. 어느 쪽이든 메시지는 실제 에이전트 턴이 됩니다.
- **허브 방식에 맞춘 전달 처리** — 원격 허브는 폴링 시점에 메시지를 전달 완료로 표시하고 `/mark-message-delivered` 엔드포인트가 없습니다. 브리지는 이 404를 재큐잉 대신 "이미 전달됨"으로 처리해 무한 재주입을 방지합니다.
- **커스텀 User-Agent** — Cloudflare가 Python urllib 기본 UA를 차단하므로 모든 요청에 `User-Agent: hermes-peers-bridge/1.0`을 사용합니다.
- **하트비트 기반 생존 판단** — 허브는 호스트 로컬 PID 확인 대신 `last_seen` 나이를 사용합니다. 죽은 등록에는 `reregister: true`를 반환해 브리지가 스스로 복구합니다.

## 문제 해결

| 증상 | 원인 / 해결 |
|------|-----------|
| 전송은 `ok: true`인데 답장이 영원히 안 옴 | 받는 머신이 구버전 `bridge.py` (주입 폴백 없음) → 그쪽 플러그인 업데이트 후 게이트웨이 재시작 |
| 피어가 메시지를 받는데 답장을 못 함 | 받는 머신의 `platform_toolsets`에 `claude-peers` 누락 |
| 메시지가 아예 주입되지 않음 | 받는 게이트웨이에 `*_HOME_CHANNEL` env 변수 누락 |
| CLI/TUI에서 보내면 `Peer X not found` | CLI 세션은 **로컬** 브로커와 통신합니다; 원격 피어는 게이트웨이 세션에서만 보입니다. 게이트웨이 피어를 경유하거나 게이트웨이 세션에서 전송하세요 |
| 같은 메시지가 반복 주입됨 | 404 허용 전달 처리가 없는 구버전 `bridge.py` → 플러그인 업데이트 |
| Cloudflare 뒤의 브로커가 요청을 거부 | 커스텀 User-Agent 헤더를 보내는 플러그인 버전인지 확인 |
| 등록한 OpenBot 에이전트가 **offline**으로 표시됨 | 자식 상태는 호스트를 따라갑니다. 호스트 데몬이 돌지 않거나 허브 연결이 끊긴 상태입니다. 그 머신의 `openbot_bridge.py` / launchd 유닛을 확인하세요. 호스트가 다시 하트비트하면 온라인으로 돌아옵니다 |
| 새로 등록한 OpenBot 에이전트에게 보낸 메시지가 늦게 도착 | 지금은 즉시 와야 합니다 — 호스트는 폴링에서 모르는 `to_id`를 보면 자식 목록을 바로 다시 읽습니다. 다음 하트비트(~15초)까지 기다리거나 영영 안 오면 그 머신의 `openbot_bridge.py`를 업데이트하세요 |
| 대시보드에서 OpenBot 에이전트 등록이 거절됨 | `host is offline…` → 호스트 데몬을 먼저 시작. `agent not found on host` → 에이전트가 최근 `openbot-cli list` 보고에 없음(다음 갱신 `OPENBOT_AGENTS_REFRESH_SECONDS`까지 기다리거나 데몬 재시작) |
| 업로드 거절(`File type … is not allowed` / `File exceeds … the byte limit`) | MIME이 허용 목록 밖이거나 `CLAUDE_PEERS_ATTACHMENT_MAX_BYTES`(기본 20 MB) 초과. 파일을 압축하거나 변환하세요 |
| 등록 시 `host not found` / `host is not an OpenBot host` | `host_id`가 실행 중인 OpenBot 호스트를 가리키지 않습니다. 데몬부터 등록하세요. Hermes 피어를 가리키면 안 됩니다 |

## 대시보드 (`/dashboard`)

`broker.remote-hub.ts`(를 `broker.ts`로)와 `dashboard.html`을 브로커 디렉터리에 함께 둡니다. 대시보드는 `GET /api/stream`·`GET /api/snapshot`으로 실시간 갱신되며, `CLAUDE_PEERS_HUB_TOKEN`이 설정되어 있으면 Bearer 토큰을 요청합니다.

- **피어 CRUD API**: `GET /api/peers`(호스트는 보고한 `agents` 포함), `POST /api/peers`(주소록 항목 또는 OpenBot 에이전트 `kind: "openbot"` + `host_id` + `agent_id`), `PATCH /api/peers/:id`, `DELETE /api/peers/:id`(온라인은 `?force=1`; 호스트 삭제 시 자식 에이전트도 삭제), `POST /api/messages`(`attachments: [id, …]` 선택), `POST /api/attachments`(multipart, 필드 `file` → `201 {id, name, size, mime, sha256}`).
- **OpenBot 등록 폼**: Kind **OpenBot agent**를 고르면 선택한 온라인 호스트의 에이전트가 나열됩니다(체크한 에이전트당 피어 1개, *Select all* 있음). 오프라인 호스트는 목록에서 비활성화되고, 호스트가 없으면 설치 패널로 안내합니다.
- **Install bridge on a machine 패널**: OpenBot 머신용 복사 가능한 단계(클론, env 파일, launchd plist + `launchctl load`, 검증)와 Linux systemd 사용자 유닛. 브로커 URL은 페이지 주소로 채워집니다. Hermes 머신은 이 README의 §2–§4로 안내합니다.

## 라이선스

MIT (플러그인 코드). 브로커는 [claude-peers-mcp](https://github.com/louislva/claude-peers-mcp)를 기반으로 합니다 — 해당 저장소의 라이선스를 참고하세요.

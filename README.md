## 설치

### 파이썬 설치 패키지 업그레이드

```bash
python -m pip install --upgrade pip setuptools wheel
```

### 현재 경로에 `gd-poller` 폴더를 만들어서 git clone

소스 업데이트도 아래 명령어로 실행하세요.

```bash
pip install --src . -e "git+https://github.com/halfaider/gd-poller.git#egg=gd_poller"
```

### 소스 코드를 직접 설치할 경우

```bash
git clone https://github.com/halfaider/gd-poller.git
pip install -r gd-poller/rquirements.txt
```

소스 코드를 업데이트 할 경우 pull 하세요.

```bash
git pull
```

## 실행

### `gd-poller` 명령어로 실행

```bash
gd-poller
```

설정 파일을 따로 지정하지 않으면 자동으로 설정 파일을 탐색합니다.

```
/data/commands/gd-poller
    /gd_poller
        __init__.py
        apis.py
        cli.py
        ...
        settings.sample.yaml
    .gitignore
    ...
    requirements.txt
```

이런 폴더 구조로 설치되어 있다고 가정할 경우 설정 파일은 아래의 순서대로 탐색됩니다.

```
/data/commands/gd-poller/gd_poller/settings.yaml
${PWD}/settings.yaml
/data/commands/gd-poller/gd_poller/config.yaml
${PWD}/config.yaml
```

### 설정 파일을 지정

설정 파일 경로를 따로 지정할 경우 아래처럼 입력하세요.

```bash
gd-poller /data/db/gd-poller.yaml
```

### 패키지 모듈을 지정해서 실행

```bash
cd gd-poller
python3 -m gd_poller.cli /path/to/settings.yaml
```

## 설정

다음은 실행에 필요한 최소한의 설정입니다. 추가적인 설정은 아래의 내용을 참고해주세요.

```yaml
google_drive:
  token:
    client_id: "123456789012-1234abcd5678efgh9012ijk3456lmno7.apps.googleusercontent.com"
    client_secret: "ABCDEF-a1b2c3d4e5f6g7h8j9k0l1m2n3o4"
    refresh_token: "1//0a1b2c3d4f5g6h7i8j9k0l1m2n3o-4p5q6r7s8t9u0v1w2x-3y4z5a6b7c8d-9e0f1g2h3i4j5k6l7m-8n9o0p1q2r3s4t5u6v7w"
    scopes:
      - "drive.readonly"
      - "drive.activity.readonly"
pollers:
  - targets:
      - "폴더_ID#/path/to/be/resolved"
```

### Google Drive 설정

`google_drive`에는 Google Drive Activity를 가져오기 위한 인증 정보를 설정합니다.

```yaml
google_drive:
  scopes:
    - "drive.readonly"
    - "drive.activity.readonly"
  token:
    client_id: "123456789012-1234abcd5678efgh9012ijk3456lmno7.apps.googleusercontent.com"
    client_secret: "ABCDEF-a1b2c3d4e5f6g7h8j9k0l1m2n3o4"
    refresh_token: "1//0a1b2c3d4f5g6h7i8j9k0l1m2n3o-4p5q6r7s8t9u0v1w2x-3y4z5a6b7c8d-9e0f1g2h3i4j5k6l7m-8n9o0p1q2r3s4t5u6v7w"
    token: "ya11.abcdef...j34kl" # 생략 가능
  cache_enable: false
  cache_ttl: 600 # 단위: 초
  cache_maxsize: 64 # 단위: 개
```

|키워드||설명|
| ------------: | :--: | -------------------------------- |
|        scopes | 필요 | 토큰 발급시 지정한 scopes 입니다. |
|     client_id | 필요 | 사용자 인증 정보에서 생성한 id 입니다. |
| client_secret | 필요 | 사용자 인증 정보에서 생성한 secret 입니다. |
| refresh_token | 필요 | 발급받은 refresh token 입니다. |
|         token |      | 발급받은 access token 입니다.<br>실행에 필요하지 않아 생략 가능합니다. |
|  cache_enable |      | 캐시 사용 여부입니다. 캐시된 데이터를 참조하면 데이터의 신뢰도가 떨어질 수도 있습니다. |
|     cache_ttl |      | API 요청시 저장할 결과물 캐시의 수명입니다. 입력하지 않을 경우 기본값은 600 초입니다. |
| cache_maxsize |      | API 요청시 저장할 결과물 캐시의 최대 개수입니다. 입력하지 않을 경우 기본값은 64 개입니다.<br>(현재 cache는 drive API의 file 메소드에만 적용되어 있습니다) |

### Pollers 설정

`pollers`는 하나 이상의 poller 리스트를 설정합니다.

```yaml
pollers:
  - targets:
      - "폴더_ID#/path/to/be/resolved"
    name: "poller의 이름"
    dispatchers: []
    polling_interval: 60
    polling_delay: 0
    dispatch_interval: 1
    buffer_interval: 30
    page_size: 100
    ignore_folder: true
    patterns:
      - ".*"
    ignore_patterns:
      - ""
    actions:
      - "create"
      - "move"
      - "delete"
      - "rename"
    task_check_interval: -1
```

|키워드||설명|
| ------------------: | :--: | ------------------------------- |
|             targets | 필요 | `activity`를 가져올 대상 폴더입니다. |
|                name |      | `poller`의 이름입니다. |
|         dispatchers |      | `activity` 정보를 전달할 대상들입니다. 복수의 `dispatcher`를 사용할 수 있고 명시한 순서대로 전달됩니다. |
|    polling_interval |      | `polling` 간격입니다. API 분당 할당량이 `100`이라는 점을 감안해서 세팅하세요. (단위: 초) |
|       polling_delay |      | `polling` 지연 시간입니다. 액티비티 조회를 `polling_delay` 시간 후에 조회합니다. 예를 들어 300초로 설정한 경우 `17:05` 까지의 액티비티는 300초 후인 `17:10` 에 조회합니다. (단위: 초) |
|   dispatch_interval |      | `dispatch` 간격입니다. 다량의 활동 내역이 한번에 `dispatch` 되는 것을 방지하기 위한 간격입니다. (단위: 초) |
|     buffer_interval |      | 이 시간(초) 간격으로 `dispatch`된 데이터를 수집후 동일한 폴더는 한번에 처리합니다. (일부 dispatcher만 지원) |
|           page_size |      | 한번에 가져올 `activity` 개수입니다. `activity` 개수가 이보다 많으면 API를 다시 소모해서 다음 페이지를 요청합니다. |
|             actions |      | 어떤 `activity`를 `dispatch`할지 설정합니다. 명시하지 않으면 모든 타입의 `activity`를 대상으로 합니다. |
|            patterns |      | 파일의 경로가 `patterns` 리스트 중 하나라도 일치하면 `dispatch` 합니다. 패턴 형식은 정규표현식으로 작성해 주시면 됩니다. 대소문자는 구분하지 않습니다. |
|     ignore_patterns |      | 파일의 경로가 `ignore_patterns` 리스트 중 하나라도 일치하면 `dispatch` 하지 않습니다. 패턴 형식은 `patterns`와 동일합니다. |
|       ignore_folder |      | 대상이 폴더일 때 `dispatch`할지 여부를 설정합니다. `true`일 경우 활동 대상이 폴더일 경우 `dispatch`하지 않습니다. |
| task_check_interval |      | 비동기 작업의 상태를 확인하는 간격입니다. 디버깅을 위해서 비동기 작업의 상태를 확인하는 용도로 사용합니다. (단위: 초) |

- targets

  구글드라이브폴더ID#지정할경로

  `#` 앞쪽의 문자는 감시할 폴더ID입니다. `#` 뒷쪽의 경로는 이 구글 드라이브의 폴더가 변환될 경로입니다.

  ```
  1LzlXqGQ...#/mnt/gds/GDRIVE/VIDEO/방송중
  ```

  `1LzlXqGQ...` 폴더내의 파일 경로는 모두 root 경로로 `/mnt/gds/GDRIVE/VIDEO/방송중`을 사용하게 됩니다.<br>다시 말해 구글 드라이브 상의 경로 `[GDS]/ROOT/GDRIVE/VIDEO/방송중`을 `/mnt/gds/GDRIVE/VIDEO/방송중`으로 변환한다는 의미입니다.<br>변환될 경로를 지정하지 않으면 `/{드라이브_ID}/ROOT/GDRIVE/VIDEO/방송중`으로 dispatcher에 전달됩니다.

- actions
  ```
  create
  edit
  move
  rename
  delete
  restore
  permissionChange
  comment
  dlpChange
  reference
  settingsChange
  appliedLabelChange
  ```

### Dispatcher 설정

`dispatcher`는 각 `poller`마다 여러개 설정할 수 있습니다. 명시한 순서대로 `dispatch` 됩니다.

```yaml
pollers:
  - targets:
      - "폴더_ID#/path/to/be/resolved"
    dispatchers:
      - class: DummyDispatcher
      - class: DiscordDispatcher
      - class: MultiServerDispatcher
      - class: CommandDispatcher
```

`class`에는 `dispatchers.py`의 여러 클래스 중에서 이 `dispatcher`에 적용할 클래스 이름을 입력합니다.

`class`외의 설정 값은 각 `dispatcher` 클래스마다 다릅니다.

#### DiscordDispatcher

```yaml
- class: DiscordDispatcher
  webhook_id: "1234567890123456789"
  webhook_token: "abcdefghijklmnopqrstuvwxyz1234567890abcdefghijklmnopqrstuvwxyz123456"
  colors:
    default: "0"
    move: "3447003"
    create: "5763719"
    delete: "15548997"
    edit: "16776960"
    rename: "16776960"
```

|키워드||설명|
| ------------: | :--: | -------------- |
|    webhook_id | 필요 | discord 웹훅 id |
| webhook_token | 필요 | discord 웹훅 token |
|        colors |      | 웹훅에 전달할 색상 지정 |
|      mappings |      | `tragets`에서 지정한 경로를 변환 |

`colors`에서 `action`별로 임베드의 색상을 지정할 수 있습니다. 색상값은 아래 링크의 `Int value`를 참고해서 넣어주세요.

https://gist.github.com/thomasbnt/b6f455e2c7d743b796917fa3c205f812

#### RcloneDispatcher

```yaml
- class: RcloneDispatcher
  url: "http://username:password@localhost:5275"
  mappings:
    - "/mnt/gds/GDRIVE:/GDRIVE"
```

|키워드||설명|
| -------: | :--: | ------------------------------- |
|      url | 필요 | rclone 리모트 콘트롤 서버의 URL 주소 |
| mappings |      | `tragets`에서 지정한 경로를 변환 |

`targets`에서 지정한 경로를 리모트 경로로 지정하면 `mappings`를 생략해도 됩니다.

```yaml
pollers:
  - targets:
      - "폴더_ID#/GDRIVE"
    dispatchers:
      - class: RcloneDispatcher
        url: "http://username:password@localhost:5275"
```
#### PlexDispatcher

```yaml
- class: PlexDispatcher
  url: "http://plex:32400"
  token: "1bCdEfG0HiJkLmNoP2Qr"
  mappings:
    - "/GDRIVE:/plex/gds2/GDRIVE"
```

|키워드||설명|
| -------: | :--: | ------------------------------- |
|      url | 필요 | plex 서버의 URL 주소 |
|    token | 필요 | plex 서버의 X-Plex-Token |
| mappings |      | `tragets`에서 지정한 경로를 변환 |

#### PlexmateDispatcher

```yaml
- class: PlexmateDispatcher
  url: "http://flaskfarm:9999"
  apikey: "ABCDEFGHI9"
  mappings:
    - "/mnt/gds/GDRIVE:/host/mnt/gds/GDRIVE"
```

`plex_mate` 플러그인으로 스캔 요청을 보냅니다.
|키워드||설명|
| ---: | :--: | --- |
|url|필요|flaskfarm의 URL 주소|
|apikey|필요|flaskfarm의 apikey|
|mappings||flaskfarm의 로컬 경로로 변환|

#### KavitaDispatcher

```yaml
- class: KavitaDispatcher
  url: "http://kavita:5000"
  apikey: "123abcde-001f-002g-003h-ijklmnop0987"
  buffer_interval: 60
  mappings:
    - "/GDRIVE:/mnt/gds2/GDRIVE"
```

카비타에 스캔 요청을 보냅니다.
|키워드||설명|
| ---: | :--: | --- |
|url|필요|카비타의 API URL 주소|
|apikey|필요|카비타의 API Key|
|buffer_interval||동일한 폴더 끼리 처리하기 위한 버퍼 시간|
|mappings||`tragets`에서 지정한 경로를 변환|

#### CommandDispatcher

```yaml
- class: CommandDispatcher
  command: "python3 /data/command/file-process.py"
  wait_for_process: false
  drop_during_process: false
  timeout: 60
  mappings:
    - "/GDRIVE:X:/downloads"
```

|키워드||설명|
| ------------------: | :--: | -------------------- |
|             command | 필요 | 실행할 쉘 커맨드입니다. |
|    wait_for_process |      | 커맨드가 종료될 때까지 대기할지 결정합니다. |
| drop_during_process |      | 커맨드가 실행 중일 때 후속으로 전달된 활동 내역에 대한 커맨드 실행 여부를 결정합니다. |
|             timeout |      | 커맨드가 이 시간동안 계속 실행될 경우 타임아웃으로 간주하고 프로세스를 종료합니다. |
|            mappings |      | 커맨드에 전달하기 전에 경로를 변환합니다. |

#### JellyfinDispatcher

```yaml
- class: JellyfinDispatcher
  url: "http://jellyfin:8096"
  apikey: "a1b2bc3d4f5g6h7i8j9k0l1m2n3o4p5q"
  buffer_interval: 60
  mappings:
    - "/GDRIVE:/jellyfin/gds2/GDRIVE"
```

젤리핀에 스캔 요청을 보냅니다.
|키워드||설명|
| ---: | :--: | --- |
|url|필요|젤리핀 서버 주소|
|apikey|필요|젤리핀 서버 API Key|
|buffer_interval||동일한 폴더 끼리 처리하기 위한 버퍼 시간|
|mappings||`tragets`에서 지정한 경로를 변환|

#### StashDispatcher

```yaml
- class: StashDispatcher
  url: "http://stash:9999"
  apikey: "eyabcdefghijklmnopqrstuvwxyz12345678.eyJa1b2c3d3f4g5h6jiasdfklzxcvmdskafjlkwerwoeiruqwoiruoasdfasdfasf334.1231231dfdfsdfwe23f3-23kdkdkdkvjvnj4j4302q0"
  buffer_interval: 60
  mappings:
    - "/GDRIVE:/stash/gds2/GDRIVE"
```

Stash 앱에 스캔 요청을 보냅니다.
|키워드||설명|
| ---: | :--: | --- |
|url|필요|Stash 서버 주소|
|apikey|필요|Stash 서버 API Key|
|buffer_interval||동일한 폴더 끼리 처리하기 위한 버퍼 시간|
|mappings||`tragets`에서 지정한 경로를 변환|

#### MultiServerDispatcher

```yaml
- class: MultiServerDispatcher
  buffer_interval: 60
  rclones:
    - url: "http://username:password@localhost:5275"
      mappings:
        - "/GDRIVE:/GDRIVE"
  plexes:
    - url: "http://plex-1:32400"
      token: "1bCdEfG0HiJkLmNoP2Qr"
      mappings:
        - "/GDRIVE:/plex1/gds2/GDRIVE"
    - url: "http://plex-2:32400"
      token: "2bCdEfG0HiJkLmNoP2Qr"
      mappings:
        - "/GDRIVE:/plex2/gds2/GDRIVE"
    - url: "http://plex-3:32400"
      token: "3bCdEfG0HiJkLmNoP2Qr"
      mappings:
        - "/GDRIVE:/plex3/gds2/GDRIVE"
  kavitas:
    - url: 'http://kavita:5000'
      apikey: '123abcde-001f-002g-003h-ijklmnop0987'
      mappings:
        - '/GDRIVE:/mnt/gds2/GDRIVE'
  jellyfins:
    - url: 'http://jellyfin:8096'
      apikey: 'a1b2bc3d4f5g6h7i8j9k0l1m2n3o4p5q'
      mappings:
        - '/GDRIVE:/jellyfin/gds2/GDRIVE'
  stashes:
    - url: 'http://stash:9999'
      apikey: 'eyabcdefghijklmnopqrstuvwxyz12345678.eyJa1b2c3d3f4g5h6jiasdfklzxcvmdskafjlkwerwoeiruqwoiruoasdfasdfasf334.1231231dfdfsdfwe23f3-23kdkdkdkvjvnj4j4302q0'
      mappings:
        - '/GDRIVE:/stash/gds2/GDRIVE'
```

`rclones`에서 지정한 리모트 서버에 순차적으로 `vfs/refresh`를 요청한 뒤 각각의 서버에 스캔을 요청합니다.
|키워드||설명|
| ---: | :--: | --- |
|buffer_interval||동일한 폴더 끼리 처리하기 위한 버퍼 시간|
|rclones||`vfs/refresh`를 요청할 리모트 서버 목록|
|plexes||스캔을 요청할 플렉스 서버 목록|
|kavitas||스캔을 요청할 플렉스 서버 목록|
|jellyfins||스캔을 요청할 플렉스 서버 목록|
|url|필요|각 서버의 URL 주소|
|token|필요|플렉스 서버의 X-Plex-Token|
|apikey|필요|서버의 API Key|
|mappings||`tragets`에서 지정한 경로를 변환|


### Global 설정

```yaml
polling_interval: 60
polling_delay: 0
dispatch_interval: 1
task_check_interval: -1
page_size: 100
ignore_folder: true
patterns: [.*]
ignore_patterns:
  [
    '\.((json|log|git/?)$|log\.)',
    '[/\\](\..+|bonus|extra|featurette|other|sample|screenshot|trailer|\[업로드\])s?',
  ]
actions: ["create", "move", "rename", "delete", "restore"]
buffer_interval: 30

google_drive: ...
pollers: ...
logging: ...
```

각 `poller`마다 반복되는 값은 글로벌로 설정하면 우선 적용됩니다. 설정값은 아래 순서로 덮어씌우기 합니다.

- 기본 설정
- Global 설정
- Poller 설정

### Logging 설정

```yaml
logging:
  level: "DEBUG"
  format: "%(levelname).3s| %(message)s <%(filename)s:%(lineno)d#%(funcName)s>"
  redacted_patterns:
    - "apikey=(.{10,36})"
    - "'apikey': '(.{10,36})'"
    - "'X-Plex-Token': '(.{20})'"
    - "'X-Plex-Token=(.{20})'"
    - "webhooks/(.+)/(.+):\\s{"
  redacted_substitute: "<REDACTED>"
```
|키워드||설명|
| ------------------: | :-: | ------------------- |
|               level |     | 표시할 최소 로그 레벨 |
|              format |     | 로그 포맷은 파이썬 로깅 포맷을 참고 |
|   redacted_patterns |     | 로그 내용 중 민감한 정보를 정규표현식에 따라 제거 |
| redacted_substitute |     | 제거 후 대체될 문자열 |

`redacted_patterns`의 정규표현식에 그룹이 지정되어 있으면 매치된 그룹만 대체됩니다. 그룹이 없는 정규표현식은 매치 문자열 전체가 대체됩니다.

노출하고 싶지 않은 apikey가 `123456789`라면 아래처럼 설정하면 됩니다.

```yaml
logging:
  redacted_patterns:
    - "123456789"
```

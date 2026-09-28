# MQTT 기반 Simple EzVille Wallpad Control (updated)

## 1. 지원 기능

  - 조명, 난방 (외출 모드), 대기전력차단, 엘리베이터콜 상태 조회 및 제어 지원
  - 대기전력소모, 현관 스위치 상태 (외출 모드, 그룹 조명) 센서 지원
  - MQTT 기반 장치 자동 Discovery 지원
  - **엘리베이터 도착 알림**: MQTT 토픽 `ezville/elevator/arrival`으로 도착 패킷 발행
  - **Addon 먹통 감지 및 자동 리셋**: 5분 이상 응답이 없거나 MQTT 재연결이 2분 이상 실패하면 자동으로 복구
  - **영구 진단 로그**: `/share/simple_mqtt_ezville_control.log`에 장애, traceback 및 10분 주기 상태를 90일간 보관

## 2. 설치 방법

  - 애드온 스토어 -> 저장소 -> [https://github.com/ktdo79/addons](https://github.com/ktdo79/addons) 추가하기 
  - MQTT 기반 Simple EzVille Wallpad Control 설치

> 사전에 MQTT Integration 및 Mosquitto Broker Addon 설치 필수

## 3. 설정 방법

### 3.1. EW11 설정

#### 3.1.1. Serial Port 설정

  - Buffer Size를 128로 변경 

#### 3.1.2. Communication Settings 설정

##### 3.1.2.1. MQTT 설정

  - +Add를 누르고 MQTT 추가 
  - Server 주소 = Home Assistant IP 주소, Port는 Mosquitto Broker 설정 Port, Buffer Size는 128 로 설정
  - Subscribe Topic는 ew11/send, Publish Topic은 ew11/recv 로 설정
  - Mosquitto Broker에 ID/Password가 있으면 MQTT Account, Password에 기입

##### 3.1.2.2. netp 설정

  - Buffer Size를 128로 변경

### 3.2. 애드온 설정

  - DEBUG (체크 박스 O/X): Debug 모드 로그
  - MQTT_LOG (체크 박스 O/X): MQTT 연결 관련 로그
  - EW11_LOG (체크 박스 O/X): EW11 연결 관련 로그
  - mode (mqtt/socket/mixed): mqtt이면 MQTT만 사용, socket이면 socket 통신만 사용, mixed면 상태 입력은 MQTT로 + 명령은 socket 사용
  - ew11_server: EW11 IP 주소
  - ew11_port: EW11 포트 (기본값 8899)
  - ew11_id: EW11 ID (EW11 리셋시 사용)
  - ew11_password: EW11 Password (EW11 리셋시 사용)
  - command_interval (초): 명령이 안 먹히는 경우 다음 명령 시도할 interval 시간 (기본값 0.5초)
  - command_retry_count (횟수): 명령이 안 먹히는 경우 최대 재시도 횟수 (기본값 20회)
  - random_backoff (체크 박스 O/X): 명령 재시도 시 jitter 방법 사용 여부 (0초 ~ command_interval초에서 random 설정)
  - discovery_delay (초): MQTT Discovery로 장치 등록 후 대기 시간 (기본값 0.1초)
  - state_loop_delay (초): State 조회 실시 간격. 짧을 수록 상태 업데이트가 빠르나 CPU 사용율 상승 (기본값 0.02초)   
  - command_loop_delay (초): HA에서 전달된 새로운 명령을 조회하는 간격. 짧을 수록 빠른 실행이 예상되나 CPU 사용율 상승 (기본값 0.02초)
  - serial_recv_dealy (초): socket mode 사용시 state를 읽어오는 간격. 짧을 수록 상태 업데이트가 빠르나 CPU 사용율 상승 (기본값 0.02초)
  - force_update_mode (체크 박스 O/X): 상태가 기존과 같으면 업데이트 하지 않으나 체크시 force_update_period마다 강제 상태 갱신 실시
  - force_update_period (초): 강제 상태 업데이트 실행 주기 (기본값 10분)
  - force_update_duration (초): 강제 상태 업데이트 실행 기간 (기본값 2초)
  - ew11_buffer_size (bytes): serial mode에서 데이터를 읽어오는 buffer size (기본값 128)
  - ew11_timeout (초): EW11이 설정 시간 이상 데이터를 읽어오지 않으면 강제 리셋 실시 (기본값 1시간)
  - diagnostic_log_file: HA 재시작 후에도 보존되는 진단 로그 파일 (기본값 `/share/simple_mqtt_ezville_control.log`)
  - diagnostic_log_days: 일 단위 진단 로그 보관 기간 (기본값 90일)
  - diagnostic_interval (초): MQTT/EW11/Queue 상태 스냅샷 기록 주기 (기본값 600초)

### 3.3. 장애 분석 로그 수집

통신 장애가 발생하면 애드온을 재시작해도 `/share`의 로그는 삭제되지 않습니다. File editor, Samba 또는 SSH로 아래 파일을 내려받아 전달해 주세요.

```text
/share/simple_mqtt_ezville_control.log
/share/simple_mqtt_ezville_control.log.YYYY-MM-DD
```

장애가 발생한 날짜의 회전 로그와 현재 로그를 함께 전달하면 MQTT 연결 해제 코드, 자동 복구 횟수, 마지막 EW11 패킷 시각, Queue 적체 및 전체 traceback을 확인할 수 있습니다. 로그에는 MQTT/EW11 비밀번호를 기록하지 않습니다.

## 4. 추가 기능 설명

### 4.1. 엘리베이터 도착 알림

EW11에서 엘리베이터 도착 패킷이 감지되면 `ezville/elevator/arrival` 토픽으로 JSON 형식의 알림이 발행됩니다.

**발행 토픽**: `ezville/elevator/arrival`

**페이로드 예시**:
```json
{
  "event": "elevator_arrival",
  "packet": "F7330143018007F6",
  "timestamp": 1702209600
}
```

**Home Assistant 활용**:
- 자동화에서 `ezville/elevator/arrival` 토픽을 구독하여 알림, 스크립트, 씬 실행 등을 트리거할 수 있습니다.
- MQTT 센서로 등록하여 상태 히스토리를 기록할 수도 있습니다.

### 4.2. Addon 먹통 감지 및 자동 리셋

Addon이 정상적으로 동작하지 않는 경우(메모리 누수, 데드락 등) 자동으로 감지하고 복구합니다.

**동작 원리**:
- 매 60초마다 마지막 HA 명령 처리 시간 또는 EW11 상태 업데이트 시간을 확인
- 5분(300초) 이상 활동이 없으면 먹통 상태로 판단
- `restart_flag` 설정하여 기존 안전 리셋 프로세스 실행 (MQTT 종료, 소켓 종료, 루프 재시작)

**관련 로그**:
- `[WARNING] Addon 먹통 감지: XX초 동안 활동 없음. 자동 리셋 트리거`
- `[DEBUG] Addon 상태 정상: 마지막 활동 이후 X.X초 경과`

**커스터마이징** (코드 수정 필요):
- `HEALTH_CHECK_INTERVAL`: 체크 주기 (기본 60초) - 감지 속도 조절
- `HEALTH_CHECK_TIMEOUT`: 타임아웃 값 (기본 300초 = 5분) - 민감도 조절


## 0.0.12 장애 복구와 운영 방법

### 감시 구조

`run.sh`는 `exec`로 독립 감시 프로세스 `watchdog.py`를 실행합니다. 이 프로세스가
통신 프로세스 `ezville.py`를 실행하고, 통신 프로세스의 **asyncio 루프에서만**
5초마다 보내는 파이프 heartbeat를 확인합니다. 스레드가 대신 heartbeat를 보내지
않으므로 이벤트 루프가 멈춰도 정상으로 보이는 문제를 피합니다.

- `watchdog_timeout`: 기본 120초, 허용 범위 30–600초.
- heartbeat 정지: SIGUSR1로 전체 스레드 스택 기록 요청 → SIGTERM → 15초 내 종료하지 않으면 SIGKILL → 새 프로세스 실행.
- 통신 프로세스 충돌/비정상 종료: 새 프로세스 실행.
- 반복 실패: 1, 2, 4초부터 최대 60초까지 대기. 5분 이상 동작하면 대기 간격을 초기화합니다.
- 애드온 중지: 감시 프로세스가 SIGTERM을 받아 자식 프로세스를 정리하고 종료합니다. 중지 요청 후 재실행하지 않습니다.

이 기능은 통신 자식 프로세스를 복구합니다. HA 자체, OS, 감시 프로세스 자체가
종료되는 장애까지 해결하지는 않습니다. HA의 애드온 실행 감시는 별도의 보조 수단입니다.

통신 세션에서도 오류를 복구합니다. 소켓 EOF/송수신 오류, MQTT publish 실패,
태스크의 예외/예상하지 못한 종료, 큐 초과를 감지하면 태스크 취소 완료와 연결 종료를
기다린 후 새 MQTT 클라이언트·소켓·큐로 시작합니다. 대기 중이던 제어 명령은 버립니다.
실패한 명령을 재부팅 후 다시 실행하여 엘리베이터 등을 중복 제어하지 않기 위해서입니다.

### 진단 파일

기본 `/share/simple_mqtt_ezville_control.log`와 0.0.11의 날짜별 회전 로그를 유지합니다.
추가로 다음 파일을 생성합니다.

- `simple_mqtt_ezville_control.log.watchdog.log`: 독립 감시기의 장애/재실행 기록. 1 MiB 회전, 이전 파일 3개.
- `simple_mqtt_ezville_control.log.stacks.log`: 이벤트 루프 정지 시 모든 스레드의 실행 위치. 프로세스 시작 때 5 MiB 초과 파일을 `.1`로 회전.

`diagnostic_log_file`을 변경하면 추가 파일도 그 경로에 접미사를 붙여 생성합니다.
파일을 만들 수 없으면 콘솔 로그로 동작합니다. 기존 일 단위 진단 로그는 바이트 수를
제한하지 않으므로 원시 패킷 DEBUG를 장기간 켜는 경우 용량을 확인하세요.

`[HEALTH]`의 `ew11_packet_age`는 실제 수신 이후 시간, `valid_packet_age`는 체크섬이
유효한 패킷 이후 시간, `state_publish_age`는 변경/강제 상태를 Paho가 수락한 이후
시간입니다. QoS 0 publish 수락은 실제 기기 반영이나 브로커의 ACK를 보장하지 않습니다.
상태가 바뀌지 않으면 `state_publish_age`가 커질 수 있습니다. `command_age`가 큰 것은
명령을 보내지 않았다는 뜻일 수 있으므로 이것만으로 장애라고 판단하지 않습니다.

기존 `ezville/ew11/status`의 `last_packet_time`은 유지하며, `last_packet_age`를 추가합니다.
일반 타임스탬프는 벽시계, 타임아웃 계산은 monotonic clock을 사용합니다.

### 입력 및 연결 정책

- 기존 MQTT topic, Discovery unique ID 및 엘리베이터 도착 패킷은 유지합니다.
- MQTT `mqtt_port` 기본값은 1883입니다. 기존 설치 옵션에 없어도 동작합니다.
- retained 제어 명령은 재연결 시 뜻하지 않게 재실행될 수 있어 무시합니다.
- 조명/콘센트는 `ON`/`OFF`, 난방은 `heat`/`off` 및 5–40도, 엘리베이터 버튼은 HA 기본값 `PRESS`, 가스밸브는 `OFF`만 허용합니다.
- 수신 큐 1000개 또는 명령 큐 100개를 초과하면 세션 복구를 수행합니다. 패킷 일부를 조용히 버리고 파싱을 계속하지 않습니다.
- MQTT 재연결 시 캐시를 비우고 이후 EW11 패킷으로 Discovery와 상태를 다시 발행합니다.

### 업데이트 및 남은 운영 작업

1. 이 변경을 저장소 기본 브랜치에 병합한 뒤 HA 앱 스토어에서 업데이트를 확인합니다.
2. 버전 0.0.12를 업데이트하고 로그에 `[START]`와 `[HEALTH]`가 출력되는지 확인합니다.
3. 평소 사용하던 조명·난방·엘리베이터 명령과 응답을 실제 장치에서 확인합니다.
4. 장애가 다시 나면 발생 시각과 진단 로그, `.watchdog.log`, `.stacks.log`를 함께 확인합니다.

기존 설치에서 확인된 별도 설정 문제: HA 자동화 `Restart Addon Daily - Wallpad` 및
`월패드 재시작`에 예전 애드온 ID `49b7d12f_simple_mqtt_ezville_control`이 남아 있어
재시작이 실패했습니다. 당시 설치 ID는 `8a83e751_simple_mqtt_ezville_control`이었습니다.
실제 설치 ID를 다시 확인하고 해당 자동화의 대상 값을 수정하세요. 이 코드 변경은
HA 자동화나 저장소 등록 URL을 자동 변경하지 않습니다.

애드온 정보의 URL은 현재 저장소로 수정했습니다. 루트 `repository.yaml`과 HA에 등록된
`hyecahn/eville` URL은 별도로 확인해야 합니다. 등록 URL을 바꾸면 저장소별 애드온 ID가
달라질 수 있으므로 단순 제거/재등록을 업데이트 절차로 사용하지 마세요.

### 검증

```sh
python -m pip install paho-mqtt==2.1.0
python -m unittest discover -s simple_mqtt_ezville_control/tests -v
```

테스트는 가짜 MQTT 클라이언트뿐 아니라 로컬 TCP 브로커와 실제 Paho 라이브러리,
로컬 EW11 소켓, 별도 프로세스 정지/충돌/종료를 사용합니다. 실제 RS485 버스와
월패드의 타이밍·전기적 충돌을 재현하지 않으므로 실기기 확인은 별도로 필요합니다.
Docker 베이스는 이번 변경에서 기존 Python 3.8 이미지를 유지했습니다. 지원되는
Python/배포판으로의 이행은 별도 검증이 필요한 후속 작업입니다.

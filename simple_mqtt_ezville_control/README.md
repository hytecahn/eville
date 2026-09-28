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

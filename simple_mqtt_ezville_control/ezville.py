import paho.mqtt.client as mqtt
import json
import time
import asyncio
import threading
import telnetlib
import socket
import random
import logging
import os
import sys
import traceback

from logging.handlers import TimedRotatingFileHandler

from threading import Thread
from queue import Queue, Empty, Full
from types import SimpleNamespace
import signal
import faulthandler
import math
from runtime import bounded_call, heartbeat_loop, cancel_tasks

# DEVICE 별 패킷 정보
RS485_DEVICE = {
    'light': {
        'state':    { 'id': '0E', 'cmd': '81' },

        'power':    { 'id': '0E', 'cmd': '41', 'ack': 'C1' }
    },
    'thermostat': {
        'state':    { 'id': '36', 'cmd': '81' },
        
        'power':    { 'id': '36', 'cmd': '43', 'ack': 'C3' },
        'away':    { 'id': '36', 'cmd': '45', 'ack': 'C5' },
        'target':   { 'id': '36', 'cmd': '44', 'ack': 'C4' }
    },
    'plug': {
        'state':    { 'id': '50', 'cmd': '81' },

        'power':    { 'id': '50', 'cmd': '43', 'ack': 'C3' }
    },
    'gasvalve': {
        'state':    { 'id': '12', 'cmd': '81' },

        'power':    { 'id': '12', 'cmd': '41', 'ack': 'C1' } # 잠그기만 가능
    },
    'batch': {
        'state':    { 'id': '33', 'cmd': '81' },

        'press':    { 'id': '33', 'cmd': '41', 'ack': 'C1' }
    }
}

# MQTT Discovery를 위한 Preset 정보
DISCOVERY_DEVICE = {
    'ids': ['ezville_wallpad',],
    'name': 'ezville_wallpad',
    'mf': 'EzVille',
    'mdl': 'EzVille Wallpad',
    'sw': 'ws/addons/ezville_wallpad',
}

# MQTT Discovery를 위한 Payload 정보
DISCOVERY_PAYLOAD = {
    'light': [ {
        '_intg': 'light',
        '~': 'ezville/light_{:0>2d}_{:0>2d}',
        'name': 'ezville_light_{:0>2d}_{:0>2d}',
        'opt': True,
        'stat_t': '~/power/state',
        'cmd_t': '~/power/command'
    } ],
    'thermostat': [ {
        '_intg': 'climate',
        '~': 'ezville/thermostat_{:0>2d}_{:0>2d}',
        'name': 'ezville_thermostat_{:0>2d}_{:0>2d}',
        'mode_cmd_t': '~/power/command',
        'mode_stat_t': '~/power/state',
        'temp_stat_t': '~/setTemp/state',
        'temp_cmd_t': '~/setTemp/command',
        'curr_temp_t': '~/curTemp/state',
#        "modes": [ "off", "heat", "fan_only" ],     # 외출 모드는 fan_only로 매핑
        'modes': [ 'heat', 'off' ],     # 외출 모드는 off로 매핑
        'min_temp': '5',
        'max_temp': '40'
    } ],
    'plug': [ {
        '_intg': 'switch',
        '~': 'ezville/plug_{:0>2d}_{:0>2d}',
        'name': 'ezville_plug_{:0>2d}_{:0>2d}',
        'stat_t': '~/power/state',
        'cmd_t': '~/power/command',
        'icon': 'mdi:leaf'
    },
    {
        '_intg': 'binary_sensor',
        '~': 'ezville/plug_{:0>2d}_{:0>2d}',
        'name': 'ezville_plug-automode_{:0>2d}_{:0>2d}',
        'stat_t': '~/auto/state',
        'icon': 'mdi:leaf'
    },
    {
        '_intg': 'sensor',
        '~': 'ezville/plug_{:0>2d}_{:0>2d}',
        'name': 'ezville_plug_{:0>2d}_{:0>2d}_powermeter',
        'stat_t': '~/current/state',
        'unit_of_meas': 'W'
    } ],
    'gasvalve': [ {
        '_intg': 'switch',
        '~': 'ezville/gasvalve_{:0>2d}_{:0>2d}',
        'name': 'ezville_gasvalve_{:0>2d}_{:0>2d}',
        'stat_t': '~/power/state',
        'cmd_t': '~/power/command',
        'icon': 'mdi:valve'
    } ],
    'batch': [ {
        '_intg': 'button',
        '~': 'ezville/batch_{:0>2d}_{:0>2d}',
        'name': 'ezville_batch-elevator-up_{:0>2d}_{:0>2d}',
        'cmd_t': '~/elevator-up/command',
        'icon': 'mdi:elevator-up'
    },
    {
        '_intg': 'button',
        '~': 'ezville/batch_{:0>2d}_{:0>2d}',
        'name': 'ezville_batch-elevator-down_{:0>2d}_{:0>2d}',
        'cmd_t': '~/elevator-down/command',
        'icon': 'mdi:elevator-down'
    },
    {
        '_intg': 'binary_sensor',
        '~': 'ezville/batch_{:0>2d}_{:0>2d}',
        'name': 'ezville_batch-groupcontrol_{:0>2d}_{:0>2d}',
        'stat_t': '~/group/state',
        'icon': 'mdi:lightbulb-group'
    },
    {
        '_intg': 'binary_sensor',
        '~': 'ezville/batch_{:0>2d}_{:0>2d}',
        'name': 'ezville_batch-outing_{:0>2d}_{:0>2d}',
        'stat_t': '~/outing/state',
        'icon': 'mdi:home-circle'
    } ]
}

# STATE 확인용 Dictionary
STATE_HEADER = {
    prop['state']['id']: (device, prop['state']['cmd'])
    for device, prop in RS485_DEVICE.items()
    if 'state' in prop
}

# ACK 확인용 Dictionary
ACK_HEADER = {
    prop[cmd]['id']: (device, prop[cmd]['ack'])
    for device, prop in RS485_DEVICE.items()
        for cmd, code in prop.items()
            if 'ack' in code
}

# 장애 분석용 영구 로그. init_diagnostic_logging() 전에는 콘솔만 사용한다.
diagnostic_logger = logging.getLogger('simple_mqtt_ezville_control')
diagnostic_logger.setLevel(logging.INFO)
diagnostic_logger.propagate = False


def init_diagnostic_logging(config):
    """HA 재시작 후에도 남는 /share 일 단위 순환 로그를 설정한다."""
    log_file = config.get('diagnostic_log_file', '/share/simple_mqtt_ezville_control.log')
    backup_count = config.get('diagnostic_log_days', 90)

    try:
        directory = os.path.dirname(log_file)
        if directory:
            os.makedirs(directory, exist_ok=True)
        handler = TimedRotatingFileHandler(
            log_file, when='midnight', backupCount=backup_count, encoding='utf-8'
        )
        handler.setFormatter(logging.Formatter('%(message)s'))
        handler.suffix = '%Y-%m-%d'
        for previous in diagnostic_logger.handlers:
            previous.close()
        diagnostic_logger.handlers.clear()
        diagnostic_logger.addHandler(handler)
    except OSError as error:
        print('[WARNING] diagnostic file unavailable; console logging only: {}'.format(error), flush=True)


# LOG 메시지
def log(string):
    date = time.strftime('%Y-%m-%d %p %I:%M:%S', time.localtime(time.time()))
    message = '[{}] {}'.format(date, string)
    print(message, flush=True)
    if diagnostic_logger.handlers:
        diagnostic_logger.info(message)
    return

# CHECKSUM 및 ADD를 마지막 4 BYTE에 추가
def checksum(input_hex):
    try:
        input_hex = input_hex[:-4]
        
        # 문자열 bytearray로 변환
        packet = bytes.fromhex(input_hex)
        
        # checksum 생성
        checksum = 0
        for b in packet:
            checksum ^= b
        
        # add 생성
        add = (sum(packet) + checksum) & 0xFF 
        
        # checksum add 합쳐서 return
        return input_hex + format(checksum, '02X') + format(add, '02X')
    except:
        return None

    
config_dir = '/data'

HA_TOPIC = 'ezville'
STATE_TOPIC = HA_TOPIC + '/{}/{}/state'
EW11_TOPIC = 'ew11'
EW11_SEND_TOPIC = EW11_TOPIC + '/send'


# Main Function
def validate_command(topics, value):
    if len(topics) != 4 or topics[0] != HA_TOPIC or topics[-1] != 'command':
        raise ValueError('command topic shape')
    parts = topics[1].split('_')
    if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
        raise ValueError('device id')
    if not (1 <= int(parts[1]) <= 9 and 1 <= int(parts[2]) <= 9):
        raise ValueError('device address out of range')
    device, action = parts[0], topics[2]
    valid = {'light': {'power': ('ON', 'OFF')}, 'plug': {'power': ('ON', 'OFF')},
             'gasvalve': {'power': ('OFF',)},
             'thermostat': {'power': ('heat', 'off'), 'setTemp': None},
             'batch': {'elevator-up': ('PRESS',), 'elevator-down': ('PRESS',)}}
    if device not in valid or action not in valid[device]:
        raise ValueError('unsupported command')
    choices = valid[device][action]
    if choices is not None and value not in choices:
        raise ValueError('unsupported value')
    if device == 'thermostat' and action == 'setTemp':
        temperature = float(value)
        if not math.isfinite(temperature) or not 5 <= temperature <= 40:
            raise ValueError('temperature out of range')


def valid_state_packet(name, packet):
    length = int(packet[8:10], 16)
    if name == 'thermostat':
        return 7 <= length <= 21 and (length - 5) % 2 == 0
    if name == 'light':
        return 2 <= length <= 10
    if name == 'plug':
        return length >= 1 and 1 <= int(packet[10:12], 16) <= 9 and length >= 1 + 3 * int(packet[10:12], 16)
    if name in ('gasvalve', 'batch'):
        return length >= 2
    return False


async def ezville_loop(config):
    
    # Log 생성 Flag
    debug = config['DEBUG_LOG']
    mqtt_log = config['MQTT_LOG']
    ew11_log = config['EW11_LOG']
    
    # 통신 모드 설정: mixed, socket, mqtt
    comm_mode = config['mode']
    
    # Socket 정보
    SOC_ADDRESS = config['ew11_server']
    SOC_PORT = config['ew11_port']
    
    # EW11 혹은 HA 전달 메시지 저장소
    MSG_QUEUE = Queue(maxsize=1000)
    
    # EW11에 보낼 Command 및 예상 Acknowledge 패킷 
    CMD_QUEUE = asyncio.Queue(maxsize=100)
    
    # State 저장용 공간
    DEVICE_STATE = {}
    
    # 이전에 전달된 패킷인지 판단을 위한 캐쉬
    MSG_CACHE = {}
    
    # MQTT Discovery Que
    DISCOVERY_DELAY = config['discovery_delay']
    DISCOVERY_LIST = []
    
    # EW11 전달 패킷 중 처리 후 남은 짜투리 패킷 저장
    RESIDUE = ''
    
    # 강제 주기적 업데이트 설정 - 매 force_update_period 마다 force_update_duration초간 HA 업데이트 실시
    FORCE_UPDATE = False
    FORCE_MODE = config['force_update_mode']
    FORCE_PERIOD = config['force_update_period']
    FORCE_DURATION = config['force_update_duration']
    
    # Command를 EW11로 보내는 방식 설정 (동시 명령 횟수, 명령 간격 및 재시도 횟수)
    CMD_INTERVAL = config['command_interval']
    CMD_RETRY_COUNT = config['command_retry_count']
    FIRST_WAITTIME = config['first_waittime']
    RANDOM_BACKOFF = config['random_backoff']
    
    # State 업데이트 루프 / Command 실행 루프 / Socket 통신으로 패킷 받아오는 루프 / Restart 필요한지 체크하는 루프의 Delay Time 설정
    STATE_LOOP_DELAY = config['state_loop_delay']
    COMMAND_LOOP_DELAY = config['command_loop_delay']
    SERIAL_RECV_DELAY = config['serial_recv_delay']
    RESTART_CHECK_DELAY = config['restart_check_delay']
    
    # EW11에 설정된 BUFFER SIZE
    EW11_BUFFER_SIZE = config['ew11_buffer_size']
    
    # EW11 동작상태 확인용 메시지 수신 시간 체크 주기 및 체크용 시간 변수
    EW11_TIMEOUT = config['ew11_timeout']
    last_received_time = time.monotonic()

    # Paho의 자동 재접속 스레드까지 멈춘 경우를 감지하기 위한 상태값
    MQTT_RECONNECT_TIMEOUT = 120
    mqtt_connected = False
    mqtt_disconnected_time = time.monotonic()
    last_mqtt_message_time = time.monotonic()
    diagnostic_interval = config.get('diagnostic_interval', 600)
    restart_count = 0
    
    # Addon 먹통 상태 감지 및 자체 리셋 설정
    last_command_time = time.monotonic()
    last_state_update_time = time.monotonic()
    last_valid_packet_time = time.monotonic()
    HEALTH_CHECK_INTERVAL = 60  # 60초마다 헬스 체크 실행
    HEALTH_CHECK_TIMEOUT = 300  # 300초(5분) 동안 활동이 없으면 먹통으로 판단
    
    # EW11 재시작 확인용 Flag
    restart_flag = False
    soc = None
    mqtt_client = None
    shutdown = asyncio.Event()
    session_loop = asyncio.get_running_loop()
  
    # MQTT Integration 활성화 확인 Flag - 단, 사용을 위해서는 MQTT Integration에서 Birth/Last Will Testament 설정 및 Retain 설정 필요
    MQTT_ONLINE = False
    
    # Addon 정상 시작 Flag
    ADDON_STARTED = False
 
    # Reboot 이후 안정적인 동작을 위한 제어 Flag
    REBOOT_CONTROL = config['reboot_control']
    REBOOT_DELAY = config['reboot_delay']

    # 시작 시 인위적인 Delay 필요시 사용
    startup_delay = 0
  

    def reset_discovery():
        DEVICE_STATE.clear()
        MSG_CACHE.clear()
        DISCOVERY_LIST.clear()

    # MQTT 통신 연결 Callback
    def on_connect(client, userdata, flags, rc, properties):
        nonlocal mqtt_connected
        nonlocal mqtt_disconnected_time

        if rc == 0:
            mqtt_connected = True
            mqtt_disconnected_time = None
            log('[INFO] MQTT Broker 연결 성공')
            session_loop.call_soon_threadsafe(reset_discovery)
            # Socket인 경우 MQTT 장치의 명령 관련과 MQTT Status (Birth/Last Will Testament) Topic만 구독
            if comm_mode == 'socket':
                client.subscribe([(HA_TOPIC + '/#', 0), ('homeassistant/status', 0)])
            # Mixed인 경우 MQTT 장치 및 EW11의 명령/수신 관련 Topic 과 MQTT Status (Birth/Last Will Testament) Topic 만 구독
            elif comm_mode == 'mixed':
                client.subscribe([(HA_TOPIC + '/#', 0), (EW11_TOPIC + '/recv', 0), ('homeassistant/status', 0)])
            # MQTT 인 경우 모든 Topic 구독
            else:
                client.subscribe([(HA_TOPIC + '/#', 0), (EW11_TOPIC + '/recv', 0), (EW11_TOPIC + '/send', 1), ('homeassistant/status', 0)])
        else:
            log('[ERROR] MQTT connect rejected: {}'.format(rc))
         
        
    # MQTT 메시지 Callback
    def on_message(client, userdata, msg):
        nonlocal MSG_QUEUE
        nonlocal MQTT_ONLINE
        nonlocal startup_delay
        nonlocal last_mqtt_message_time
        nonlocal last_received_time
        nonlocal restart_flag

        last_mqtt_message_time = time.monotonic()
        
        if msg.topic == 'homeassistant/status':
            # Reboot Control 사용 시 MQTT Integration의 Birth/Last Will Testament Topic은 바로 처리
            if REBOOT_CONTROL:
                status = msg.payload.decode('utf-8', errors='replace')
                
                if status == 'online':
                    log('[INFO] MQTT Integration 온라인')
                    MQTT_ONLINE = True
                    if not msg.retain:
                        log('[INFO] MQTT Birth Message가 Retain이 아니므로 정상화까지 Delay 부여')
                        startup_delay = REBOOT_DELAY
                elif status == 'offline':
                    log('[INFO] MQTT Integration 오프라인')
                    MQTT_ONLINE = False
        # 나머지 topic은 모두 Queue에 보관
        elif msg.topic == EW11_TOPIC + '/recv' or (
                msg.topic.startswith(HA_TOPIC + '/') and msg.topic.endswith('/command')):
            if msg.topic == EW11_TOPIC + '/recv':
                last_received_time = time.monotonic()
            if msg.retain and msg.topic.endswith('/command'):
                log('[WARNING] retained control command ignored')
                return
            try:
                MSG_QUEUE.put_nowait(msg)
            except Full:
                # Loss of a byte-stream fragment invalidates RESIDUE. Recover the
                # whole session, rather than interpreting an incomplete stream.
                if not restart_flag:
                    log('[ERROR] receive queue overflow; restarting session')
                restart_flag = True
 

    # MQTT 통신 연결 해제 Callback
    def on_disconnect(client, userdata, disconnect_flags, rc, properties):
        nonlocal mqtt_connected
        nonlocal mqtt_disconnected_time

        mqtt_connected = False
        if mqtt_disconnected_time is None:
            mqtt_disconnected_time = time.monotonic()
        log('[WARNING] MQTT 연결 해제 (rc={})'.format(rc))

    def on_mqtt_log(client, userdata, level, buf):
        # WARNING/ERROR는 항상 보존한다. MQTT_LOG를 켜도 EW11의 고빈도
        # PUBLISH wire trace는 제외하여 정상 운용 중 로그 폭증을 막는다.
        if level in (mqtt.MQTT_LOG_WARNING, mqtt.MQTT_LOG_ERR):
            log('[MQTT] level={} {}'.format(level, buf))
            return
        if not mqtt_log:
            return
        # Paho는 수신/송신 PUBLISH마다 DEBUG 로그를 발생시킨다. EW11/recv는
        # 초당 여러 번 들어올 수 있으므로 payload wire trace는 별도 EW11_LOG로 본다.
        if buf.startswith('Received PUBLISH') or buf.startswith('Sending PUBLISH'):
            return
        log('[MQTT] level={} {}'.format(level, buf))


    # MQTT message를 분류하여 처리
    async def process_message():
        # A busy producer must not monopolize the event loop forever.
        for _ in range(100):
            try:
                msg = MSG_QUEUE.get_nowait()
            except Empty:
                break
            topics = msg.topic.split('/')
            if topics[0] == HA_TOPIC and topics[-1] == 'command':
                try:
                    await HA_process(topics, msg.payload.decode('utf-8'))
                except (ValueError, IndexError, UnicodeError) as error:
                    log('[WARNING] invalid command rejected: {}'.format(type(error).__name__))
            elif msg.topic == EW11_TOPIC + '/recv':
                await EW11_process(msg.payload.hex().upper())
            await asyncio.sleep(0)
    
    # EW11 전달된 메시지 처리
    async def EW11_process(raw_data):
        nonlocal DISCOVERY_LIST
        nonlocal RESIDUE
        nonlocal MSG_CACHE
        nonlocal DEVICE_STATE
        nonlocal last_valid_packet_time
        
        raw_data = RESIDUE + raw_data
        RESIDUE = ''
        
        if ew11_log:
            log('[SIGNAL] receved: {}'.format(raw_data))
        
        k = 0
        cors = []
        msg_length = len(raw_data)
        while k < msg_length:
            # F7로 시작하는 패턴을 패킷으로 분리
            if raw_data[k:k + 2] == 'F7':
                # 남은 데이터가 최소 패킷 길이를 만족하지 못하면 RESIDUE에 저장 후 종료
                if k + 10 > msg_length:
                    RESIDUE = raw_data[k:]
                    break
                else:
                    data_length = int(raw_data[k + 8:k + 10], 16)
                    packet_length = 10 + data_length * 2 + 4 
                    
                    # 남은 데이터가 예상되는 패킷 길이보다 짧으면 RESIDUE에 저장 후 종료
                    if k + packet_length > msg_length:
                        RESIDUE = raw_data[k:]
                        break
                    else:
                        packet = raw_data[k:k + packet_length]
                        
                # 분리된 패킷이 Valid한 패킷인지 Checksum 확인                
                if packet != checksum(packet):
                    k+=1
                    continue
                else:
                    last_valid_packet_time = time.monotonic()
                    STATE_PACKET = False
                    ACK_PACKET = False
                    
                    # STATE 패킷인지 확인
                    if packet[2:4] in STATE_HEADER and packet[6:8] in STATE_HEADER[packet[2:4]][1]:
                        STATE_PACKET = True
                    # ACK 패킷인지 확인
                    elif packet[2:4] in ACK_HEADER and packet[6:8] in ACK_HEADER[packet[2:4]][1]:
                        ACK_PACKET = True
                    
                    # 엘리베이터 도착 패킷 감지: F7330143018007F6 (고정 14문자)
                    # 패킷 분석: F7=시작, 33=엘리베이터, 0143=도착명령, 018007=도착상태, F6=체크섬/add
                    if packet == 'F7330143018007F6':
                        try:
                            payload = {'event': 'elevator_arrival', 'packet': packet, 'timestamp': int(time.time())}
                            mqtt_client.publish(HA_TOPIC + '/elevator/arrival', json.dumps(payload))
                            log('[ALERT] Elevator arrival detected: {}'.format(packet))
                            if mqtt_log:
                                log('[LOG] ->> HA : {} >> {}'.format(HA_TOPIC + '/elevator/arrival', json.dumps(payload)))
                        except Exception as _e:
                            log('[ERROR] Elevator arrival publish failed: {}'.format(_e))

                    if STATE_PACKET or ACK_PACKET:
                        # MSG_CACHE에 없는 새로운 패킷이거나 FORCE_UPDATE 실행된 경우만 실행
                        if MSG_CACHE.get(packet[0:10]) != packet[10:] or FORCE_UPDATE:
                            name = STATE_HEADER[packet[2:4]][0]                            
                            if not valid_state_packet(name, packet):
                                k += packet_length
                                RESIDUE = ''
                                continue
                            if name == 'light':
                                # ROOM ID
                                rid = int(packet[5], 16)
                                # ROOM의 light 갯수 + 1
                                slc = int(packet[8:10], 16) 
                                
                                for id in range(1, slc):
                                    discovery_name = '{}_{:0>2d}_{:0>2d}'.format(name, rid, id)
                                    
                                    if discovery_name not in DISCOVERY_LIST:
                                        DISCOVERY_LIST.append(discovery_name)
                                    
                                        payload = DISCOVERY_PAYLOAD[name][0].copy()
                                        payload['~'] = payload['~'].format(rid, id)
                                        payload['name'] = payload['name'].format(rid, id)
                                   
                                        # 장치 등록 후 DISCOVERY_DELAY초 후에 State 업데이트
                                        await mqtt_discovery(payload)
                                        await asyncio.sleep(DISCOVERY_DELAY)
                                    
                                    # State 업데이트까지 진행
                                    onoff = 'ON' if int(packet[10 + 2 * id: 12 + 2 * id], 16) > 0 else 'OFF'
                                        
                                    await update_state(name, 'power', rid, id, onoff)
                                    
                                    # 직전 처리 State 패킷은 저장
                                    if STATE_PACKET:
                                        MSG_CACHE[packet[0:10]] = packet[10:]
                                                                                    
                            elif name == 'thermostat':
                                # room 갯수
                                rc = int((int(packet[8:10], 16) - 5) / 2)
                                # room의 조절기 수 (현재 하나 뿐임)
                                src = 1
                                
                                onoff_state = bin(int(packet[12:14], 16))[2:].zfill(8)
                                away_state = bin(int(packet[14:16], 16))[2:].zfill(8)
                                
                                for rid in range(1, rc + 1):
                                    discovery_name = '{}_{:0>2d}_{:0>2d}'.format(name, rid, src)
                                    
                                    if discovery_name not in DISCOVERY_LIST:
                                        DISCOVERY_LIST.append(discovery_name)
                                    
                                        payload = DISCOVERY_PAYLOAD[name][0].copy()
                                        payload['~'] = payload['~'].format(rid, src)
                                        payload['name'] = payload['name'].format(rid, src)
                                   
                                        # 장치 등록 후 DISCOVERY_DELAY초 후에 State 업데이트
                                        await mqtt_discovery(payload)
                                        await asyncio.sleep(DISCOVERY_DELAY)
                                    
                                    onoff = 'off'
                                    setT = str(int(packet[16 + 4 * rid:18 + 4 * rid], 16))
                                    curT = str(int(packet[18 + 4 * rid:20 + 4 * rid], 16))
                                    
                                    if onoff_state[8 - rid ] == '1':
                                        onoff = 'heat'
                                    # 외출 모드는 off로 
                                    elif onoff_state[8 - rid] == '0' and away_state[8 - rid] == '1':
                                        onoff = 'off'
#                                    elif onoff_state[8 - rid] == '0' and away_state[8 - rid] == '0':
#                                        onoff = 'off'
#                                    else:
#                                        onoff = 'off'

                                    await update_state(name, 'power', rid, src, onoff)
                                    await update_state(name, 'curTemp', rid, src, curT)
                                    await update_state(name, 'setTemp', rid, src, setT)
                                    
                                # 직전 처리 State 패킷은 저장
                                if STATE_PACKET:
                                    MSG_CACHE[packet[0:10]] = packet[10:]
                                else:
                                    # Ack 패킷도 State로 저장
                                    MSG_CACHE['F7361F810F'] = packet[10:]
                                        
                            # plug는 ACK PACKET에 상태 정보가 없으므로 STATE_PACKET만 처리
                            elif name == 'plug' and STATE_PACKET:
                                if STATE_PACKET:
                                    # ROOM ID
                                    rid = int(packet[5], 16)
                                    # ROOM의 plug 갯수
                                    spc = int(packet[10:12], 16) 
                                
                                    for id in range(1, spc + 1):
                                        discovery_name = '{}_{:0>2d}_{:0>2d}'.format(name, rid, id)

                                        if discovery_name not in DISCOVERY_LIST:
                                            DISCOVERY_LIST.append(discovery_name)
                                    
                                            for payload_template in DISCOVERY_PAYLOAD[name]:
                                                payload = payload_template.copy()
                                                payload['~'] = payload['~'].format(rid, id)
                                                payload['name'] = payload['name'].format(rid, id)
                                   
                                                # 장치 등록 후 DISCOVERY_DELAY초 후에 State 업데이트
                                                await mqtt_discovery(payload)
                                                await asyncio.sleep(DISCOVERY_DELAY)  
                                    
                                        # BIT0: 대기전력 On/Off, BIT1: 자동모드 On/Off
                                        # 위와 같지만 일단 on-off 여부만 판단
                                        onoff = 'ON' if int(packet[7 + 6 * id], 16) > 0 else 'OFF'
                                        autoonoff = 'ON' if int(packet[6 + 6 * id], 16) > 0 else 'OFF'
                                        power_num = '{:.2f}'.format(int(packet[8 + 6 * id: 12 + 6 * id], 16) / 100)
                                        
                                        await update_state(name, 'power', rid, id, onoff)
                                        await update_state(name, 'auto', rid, id, autoonoff)
                                        await update_state(name, 'current', rid, id, power_num)
                                    
                                        # 직전 처리 State 패킷은 저장
                                        MSG_CACHE[packet[0:10]] = packet[10:]
                                else:
                                    # ROOM ID
                                    rid = int(packet[5], 16)
                                    # ROOM의 plug 갯수
                                    sid = int(packet[10:12], 16) 
                                
                                    onoff = 'ON' if int(packet[13], 16) > 0 else 'OFF'
                                    
                                    await update_state(name, 'power', rid, id, onoff)
                                        
                            elif name == 'gasvalve':
                                # Gas Value는 하나라서 강제 설정
                                rid = 1
                                # Gas Value는 하나라서 강제 설정
                                spc = 1 
                                
                                discovery_name = '{}_{:0>2d}_{:0>2d}'.format(name, rid, spc)
                                    
                                if discovery_name not in DISCOVERY_LIST:
                                    DISCOVERY_LIST.append(discovery_name)
                                    
                                    payload = DISCOVERY_PAYLOAD[name][0].copy()
                                    payload['~'] = payload['~'].format(rid, spc)
                                    payload['name'] = payload['name'].format(rid, spc)
                                   
                                    # 장치 등록 후 DISCOVERY_DELAY초 후에 State 업데이트
                                    await mqtt_discovery(payload)
                                    await asyncio.sleep(DISCOVERY_DELAY)                                

                                onoff = 'ON' if int(packet[12:14], 16) == 1 else 'OFF'
                                        
                                await update_state(name, 'power', rid, spc, onoff)
                                
                                # 직전 처리 State 패킷은 저장
                                if STATE_PACKET:
                                    MSG_CACHE[packet[0:10]] = packet[10:]
                            
                            # 일괄차단기 ACK PACKET은 상태 업데이트에 반영하지 않음
                            elif name == 'batch' and STATE_PACKET:
                                # 일괄차단기는 하나라서 강제 설정
                                rid = 1
                                # 일괄차단기는 하나라서 강제 설정
                                sbc = 1
                                
                                discovery_name = '{}_{:0>2d}_{:0>2d}'.format(name, rid, sbc)
                                
                                if discovery_name not in DISCOVERY_LIST:
                                    DISCOVERY_LIST.append(discovery_name)
                                    
                                    for payload_template in DISCOVERY_PAYLOAD[name]:
                                        payload = payload_template.copy()
                                        payload['~'] = payload['~'].format(rid, sbc)
                                        payload['name'] = payload['name'].format(rid, sbc)
                                   
                                        # 장치 등록 후 DISCOVERY_DELAY초 후에 State 업데이트
                                        await mqtt_discovery(payload)
                                        await asyncio.sleep(DISCOVERY_DELAY)           

                                # 일괄 차단기는 버튼 상태 변수 업데이트
                                states = bin(int(packet[12:14], 16))[2:].zfill(8)
                                        
                                ELEVDOWN = states[2]                                        
                                ELEVUP = states[3]
                                GROUPON = states[5]
                                OUTING = states[6]
                                                                    
                                grouponoff = 'ON' if GROUPON == '1' else 'OFF'
                                outingonoff = 'ON' if OUTING == '1' else 'OFF'
                                
                                #ELEVDOWN과 ELEVUP은 직접 DEVICE_STATE에 저장
                                elevdownonoff = 'ON' if ELEVDOWN == '1' else 'OFF'
                                elevuponoff = 'ON' if ELEVUP == '1' else 'OFF'
                                DEVICE_STATE['batch_01_01elevator-up'] = elevuponoff
                                DEVICE_STATE['batch_01_01elevator-down'] = elevdownonoff
                                    
                                # 일괄 조명 및 외출 모드는 상태 업데이트
                                await update_state(name, 'group', rid, sbc, grouponoff)
                                await update_state(name, 'outing', rid, sbc, outingonoff)
                                
                                MSG_CACHE[packet[0:10]] = packet[10:]
                                                                                    
                RESIDUE = ''
                k = k + packet_length
                
            else:
                k+=1
                
    
    # MQTT Discovery로 장치 자동 등록
    async def mqtt_discovery(payload):
        intg = payload.pop('_intg')

        # MQTT 통합구성요소에 등록되기 위한 추가 내용
        payload['device'] = DISCOVERY_DEVICE
        payload['uniq_id'] = payload['name']

        # Discovery에 등록
        topic = 'homeassistant/{}/ezville_wallpad/{}/config'.format(intg, payload['name'])
        log('[INFO] 장치 등록:  {}'.format(topic))
        publish_checked(topic, json.dumps(payload))

    
    # 장치 State를 MQTT로 Publish
    async def update_state(device, state, id1, id2, value):
        nonlocal DEVICE_STATE
        nonlocal last_state_update_time

        deviceID = '{}_{:0>2d}_{:0>2d}'.format(device, id1, id2)
        key = deviceID + state
        
        if value != DEVICE_STATE.get(key) or FORCE_UPDATE:
            topic = STATE_TOPIC.format(deviceID, state)
            publish_checked(topic, value.encode())
            DEVICE_STATE[key] = value
            last_state_update_time = time.monotonic()
                    
            if mqtt_log:
                log('[LOG] ->> HA : {} >> {}'.format(topic, value))

        return

    
    # HA에서 전달된 메시지 처리        
    async def HA_process(topics, value):
        nonlocal CMD_QUEUE
        nonlocal last_command_time

        validate_command(topics, value)
        device_info = topics[1].split('_')
        device = device_info[0]
        last_command_time = time.monotonic()
        
        if mqtt_log:
            log('[LOG] HA ->> : {} -> {}'.format('/'.join(topics), value))

        if device in RS485_DEVICE:
            key = topics[1] + topics[2]
            idx = int(device_info[1])
            sid = int(device_info[2])
            cur_state = DEVICE_STATE.get(key)
            
            if value == cur_state:
                pass
            
            else:
                if device == 'thermostat':                        
                    if topics[2] == 'power':
                        if value == 'heat':
                            
                            sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '01010000')
                            recvcmd = 'F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']
                            statcmd = [key, value]
                           
                            CMD_QUEUE.put_nowait({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                        
                        # Thermostat는 외출 모드를 Off 모드로 연결
                        elif value == 'off':
 
                            sendcmd = checksum('F7' + RS485_DEVICE[device]['away']['id'] + '1' + str(idx) + RS485_DEVICE[device]['away']['cmd'] + '01010000')
                            recvcmd = 'F7' + RS485_DEVICE[device]['away']['id'] + '1' + str(idx) + RS485_DEVICE[device]['away']['ack']
                            statcmd = [key, value]
                           
                            CMD_QUEUE.put_nowait({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                        
#                        elif value == 'off':
#                        
#                            sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '01000000')
#                            recvcmd = 'F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']
#                            statcmd = [key, value]
#                           
#                            CMD_QUEUE.put_nowait({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                                               
                        if debug:
                            log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                                    
                    elif topics[2] == 'setTemp':                            
                        value = int(float(value))
   
                        sendcmd = checksum('F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['cmd'] + '01' + "{:02X}".format(value) + '0000')
                        recvcmd = 'F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['ack']
                        statcmd = [key, str(value)]

                        CMD_QUEUE.put_nowait({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
                        if debug:
                            log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))

#                    elif device == 'Fan':
#                        if topics[2] == 'power':
#                            sendcmd = DEVICE_LISTS[device][idx].get('command' + value)
#                            recvcmd = DEVICE_LISTS[device][idx].get('state' + value) if value == 'ON' else [
#                                DEVICE_LISTS[device][idx].get('state' + value)]
#                            QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
#                            if debug:
#                                log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))
#                        elif topics[2] == 'speed':
#                            speed_list = ['LOW', 'MEDIUM', 'HIGH']
#                            if value in speed_list:
#                                index = speed_list.index(value)
#                                sendcmd = DEVICE_LISTS[device][idx]['CHANGE'][index]
#                                recvcmd = [DEVICE_LISTS[device][idx]['stateON'][index]]
#                                QUEUE.append({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'count': 0})
#                                if debug:
#                                    log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}'.format(sendcmd, recvcmd))

                elif device == 'light':                         
                    pwr = '01' if value == 'ON' else '00'
                        
                    sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '030' + str(sid) + pwr + '000000')
                    recvcmd = 'F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']
                    statcmd = [key, value]
                    
                    CMD_QUEUE.put_nowait({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
                    if debug:
                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                                
                elif device == 'plug':                         
                    pwr = '01' if value == 'ON' else '00'

                    sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '020' + str(sid) + pwr + '0000')
                    recvcmd = 'F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']
                    statcmd = [key, value]
                        
                    CMD_QUEUE.put_nowait({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
                    if debug:
                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                                
                elif device == 'gasvalve':
                    # 가스 밸브는 ON 제어를 받지 않음
                    if value == 'OFF':
                        sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '0' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '0100' + '0000')
                        recvcmd = ['F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']]
                        statcmd = [key, value]

                        CMD_QUEUE.put_nowait({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
                        if debug:
                            log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                                
                elif device == 'batch':
                    # Batch는 Elevator 및 외출/그룹 조명 버튼 상태 고려 
                    elup_state = '1' if DEVICE_STATE.get(topics[1] + 'elevator-up') == 'ON' else '0'
                    eldown_state = '1' if DEVICE_STATE.get(topics[1] + 'elevator-down') == 'ON' else '0'
                    out_state = '1' if DEVICE_STATE.get(topics[1] + 'outing') == 'ON' else '0'
                    group_state = '1' if DEVICE_STATE.get(topics[1] + 'group') == 'ON' else '0'

                    cur_state = DEVICE_STATE.get(key)

                    # 일괄 차단기는 4가지 모드로 조절               
                    if topics[2] == 'elevator-up':
                        elup_state = '1'
                    elif topics[2] == 'elevator-down':
                        eldown_state = '1'
# 그룹 조명과 외출 모드 설정은 테스트 후에 추가 구현                                                
#                    elif topics[2] == 'group':
#                        group_state = '1'
#                    elif topics[2] == 'outing':
#                        out_state = '1'
                            
                    CMD = '{:0>2X}'.format(int('00' + eldown_state + elup_state + '0' + group_state + out_state + '0', 2))
                    
                    # 일괄 차단기는 state를 변경하여 제공해서 월패드에서 조작하도록 해야함
                    # 월패드의 ACK는 무시
                    sendcmd = checksum('F7' + RS485_DEVICE[device]['state']['id'] + '0' + str(idx) + RS485_DEVICE[device]['state']['cmd'] + '0300' + CMD + '000000')
                    recvcmd = 'NULL'
                    statcmd = [key, 'NULL']
                    
                    CMD_QUEUE.put_nowait({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                    
                    if debug:
                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
  
                                                
    # HA에서 전달된 명령을 EW11 패킷으로 전송
    async def send_to_ew11(send_data):
            
        for i in range(CMD_RETRY_COUNT):
            if ew11_log:
                log('[SIGNAL] 신호 전송: {}'.format(send_data))
                        
            if comm_mode == 'mqtt':
                publish_checked(EW11_SEND_TOPIC, bytes.fromhex(send_data['sendcmd']))
            else:
                await asyncio.wait_for(
                    session_loop.sock_sendall(soc, bytes.fromhex(send_data['sendcmd'])),
                    timeout=10,
                )
            if debug:                     
                log('[DEBUG] Iter. No.: {}, Target: {}, Current: {}'.format(i + 1, send_data['statcmd'][1], DEVICE_STATE.get(send_data['statcmd'][0])))
             
            # Ack나 State 업데이트가 불가한 경우 한번만 명령 전송 후 Return
            if send_data['statcmd'][1] == 'NULL':
                return
      
            # FIRST_WAITTIME초는 ACK 처리를 기다림 (초당 30번 데이터가 들어오므로 ACK 못 받으면 후속 처리 시작)
            if i == 0:
                await asyncio.sleep(FIRST_WAITTIME)
            # 이후에는 정해진 간격 혹은 Random Backoff 시간 간격을 주고 ACK 확인
            else:
                if RANDOM_BACKOFF:
                    await asyncio.sleep(random.randint(0, int(CMD_INTERVAL * 1000))/1000)    
                else:
                    await asyncio.sleep(CMD_INTERVAL)
              
            if send_data['statcmd'][1] == DEVICE_STATE.get(send_data['statcmd'][0]):
                return

        log('[WARNING] command failed after {} attempts: {}'.format(CMD_RETRY_COUNT, send_data['statcmd']))
        
                                                
    # EW11 동작 상태를 체크해서 필요시 리셋 실시 및 HA에 상태 알림
    async def ew11_health_loop():
        nonlocal restart_flag
        
        ew11_status = 'online'  # EW11 상태 추적: 'online' 또는 'offline'
        
        while True:
            timestamp = time.monotonic()
            time_since_last_packet = timestamp - last_received_time
        
            # TIMEOUT 시간 동안 새로 받은 EW11 패킷이 없으면 상태를 'offline'으로 변경 및 HA에 알림
            if time_since_last_packet > EW11_TIMEOUT:
                if ew11_status == 'online':
                    # 처음 타임아웃 감지 시 HA에 알림
                    ew11_status = 'offline'
                    log('[WARNING] EW11 패킷 수신 타임아웃: {:.1f}초 동안 신호 없음'.format(time_since_last_packet))
                    
                    # HA에 EW11 상태 알림 (MQTT publish)
                    try:
                        payload = {'status': 'offline', 'timestamp': int(time.time()), 'last_packet_time': int(time.time() - time_since_last_packet), 'last_packet_age': time_since_last_packet}
                        mqtt_client.publish(HA_TOPIC + '/ew11/status', json.dumps(payload))
                        log('[ALERT] EW11 offline 상태를 HA에 전송')
                        if mqtt_log:
                            log('[LOG] ->> HA : {} >> {}'.format(HA_TOPIC + '/ew11/status', json.dumps(payload)))
                    except Exception as _e:
                        log('[ERROR] EW11 상태 publish 실패: {}'.format(_e))
                    
                    # EW11 재시작 시도
                    try:
                        log('[INFO] EW11 기기 재시작 시도')
                        await asyncio.wait_for(reset_EW11(), timeout=90)
                    except Exception as _e:
                        log('[ERROR] EW11 기기 재시작 오류: {}'.format(_e))
                    finally:
                        # Telnet 리셋의 성공 여부와 관계없이 로컬 통신 task도
                        # 반드시 새로 만들어 정지된 MQTT/socket 상태를 복구한다.
                        restart_flag = True
            else:
                if ew11_status == 'offline':
                    # 타임아웃 해제되어 다시 online 상태로 변경 시 HA에 알림
                    ew11_status = 'online'
                    log('[INFO] EW11 패킷 수신 복구됨')
                    
                    # HA에 EW11 복구 상태 알림
                    try:
                        payload = {'status': 'online', 'timestamp': int(time.time())}
                        mqtt_client.publish(HA_TOPIC + '/ew11/status', json.dumps(payload))
                        log('[INFO] EW11 online 상태를 HA에 전송')
                        if mqtt_log:
                            log('[LOG] ->> HA : {} >> {}'.format(HA_TOPIC + '/ew11/status', json.dumps(payload)))
                    except Exception as _e:
                        log('[ERROR] EW11 상태 publish 실패: {}'.format(_e))
                else:
                    if debug:
                        log('[DEBUG] EW11 연결 상태 정상: {:.1f}초 전 패킷 수신'.format(time_since_last_packet))
            
            # EW11_TIMEOUT이 길어도 복구 조건은 최대 60초마다 확인한다.
            await asyncio.sleep(min(EW11_TIMEOUT, 60))

                                                
    # Telnet 접속하여 EW11 리셋        
    async def reset_EW11(): 
        def reset():
            with telnetlib.Telnet(config['ew11_server'], timeout=10) as ew11:
                def expect(prompt):
                    if not ew11.read_until(prompt, timeout=5).endswith(prompt):
                        raise TimeoutError('EW11 Telnet prompt timeout')
                expect(b'login:')
                ew11.write(config['ew11_id'].encode('utf-8') + b'\n')
                expect(b'password:')
                ew11.write(config['ew11_password'].encode('utf-8') + b'\n')
                ew11.write(b'Restart\n')
                expect(b'Restart..')
        await bounded_call(reset, 30)
        log('[INFO] EW11 리셋 완료')
        await asyncio.sleep(60)
        
    # Addon 먹통 상태 감지 및 자동 리셋
    async def addon_health_loop():
        nonlocal restart_flag
        
        while True:
            timestamp = time.monotonic()
            
            # Incoming commands or corrupt bytes cannot prove that EW11 is healthy.
            # Unchanged but valid packets still prove that the bus is alive.
            time_since_activity = timestamp - last_valid_packet_time
            
            if time_since_activity > HEALTH_CHECK_TIMEOUT:
                log('[WARNING] Addon 먹통 감지: {}초 동안 유효 패킷 없음. 자동 리셋 트리거'.format(int(time_since_activity)))
                log('[DEBUG] Last command time: {}, Last state update time: {}'.format(last_command_time, last_state_update_time))
                restart_flag = True
                await asyncio.sleep(0)
            else:
                if debug:
                    log('[DEBUG] Addon 상태 정상: 마지막 활동 이후 {:.1f}초 경과 (타임아웃: {}초)'.format(time_since_activity, HEALTH_CHECK_TIMEOUT))
            
            # HEALTH_CHECK_INTERVAL 초마다 체크
            await asyncio.sleep(HEALTH_CHECK_INTERVAL)

    async def mqtt_health_loop():
        """Paho의 자동 재접속이 멈추면 전체 통신 루프를 재생성한다."""
        nonlocal restart_flag

        while True:
            if (not mqtt_connected and mqtt_disconnected_time is not None and
                    time.monotonic() - mqtt_disconnected_time > MQTT_RECONNECT_TIMEOUT):
                log('[WARNING] MQTT 연결이 {}초 이상 복구되지 않아 자동 리셋 트리거'.format(
                    MQTT_RECONNECT_TIMEOUT))
                restart_flag = True
            await asyncio.sleep(10)

    async def diagnostic_heartbeat_loop():
        """먹통 전후 상태를 사후 비교할 수 있도록 주기적 스냅샷을 남긴다."""
        while True:
            now = time.monotonic()
            log('[HEALTH] mqtt_connected={} mqtt_message_age={:.1f}s '
                'ew11_packet_age={:.1f}s valid_packet_age={:.1f}s state_publish_age={:.1f}s command_age={:.1f}s '
                'msg_queue={} cmd_queue={} restart_flag={} restart_count={}'.format(
                    mqtt_connected,
                    now - last_mqtt_message_time,
                    now - last_received_time,
                    now - last_valid_packet_time,
                    now - last_state_update_time,
                    now - last_command_time,
                    MSG_QUEUE.qsize(),
                    CMD_QUEUE.qsize(),
                    restart_flag,
                    restart_count))
            await asyncio.sleep(diagnostic_interval)
    
    async def initiate_socket():
        log('[INFO] Socket 연결을 시작합니다')
        connection = socket.socket()
        connection.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        connection.setblocking(False)
        try:
            await asyncio.wait_for(session_loop.sock_connect(connection, (SOC_ADDRESS, SOC_PORT)), 10)
            return connection
        except BaseException:
            connection.close()
            raise

    async def serial_recv_loop():
        nonlocal last_received_time
        while True:
            data = await asyncio.wait_for(session_loop.sock_recv(soc, EW11_BUFFER_SIZE), EW11_TIMEOUT)
            if not data:
                raise ConnectionError('EW11 socket EOF')
            last_received_time = time.monotonic()
            # A new object is essential: queued messages must never share payload.
            MSG_QUEUE.put_nowait(SimpleNamespace(topic=EW11_TOPIC + '/recv', payload=data))
            await asyncio.sleep(SERIAL_RECV_DELAY) 
        
    async def state_update_loop():
        nonlocal force_target_time
        nonlocal force_stop_time
        nonlocal FORCE_UPDATE
        
        while True:
            await process_message()                    
            
            timestamp = time.monotonic()
            
            # 정해진 시간이 지나면 FORCE 모드 발동
            if timestamp > force_target_time and not FORCE_UPDATE and FORCE_MODE:
                force_stop_time = timestamp + FORCE_DURATION
                FORCE_UPDATE = True
                log('[INFO] 상태 강제 업데이트 실시')
                
            # 정해진 시간이 지나면 FORCE 모드 종료    
            if timestamp > force_stop_time and FORCE_UPDATE and FORCE_MODE:
                force_target_time = timestamp + FORCE_PERIOD
                FORCE_UPDATE = False
                log('[INFO] 상태 강제 업데이트 종료')
                
            # STATE_LOOP_DELAY 초 대기 후 루프 진행
            await asyncio.sleep(STATE_LOOP_DELAY)
            
            
    async def command_loop():
        nonlocal CMD_QUEUE
        
        while True:
            if not CMD_QUEUE.empty():
                send_data = await CMD_QUEUE.get()
                await send_to_ew11(send_data)               
            
            # COMMAND_LOOP_DELAY 초 대기 후 루프 진행
            await asyncio.sleep(COMMAND_LOOP_DELAY)    
 

    def publish_checked(topic, payload):
        result = mqtt_client.publish(topic, payload)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            raise ConnectionError('MQTT publish rejected rc={}'.format(result.rc))

    async def restart_control():
        while not restart_flag and not (REBOOT_CONTROL and ADDON_STARTED and not MQTT_ONLINE):
            await asyncio.sleep(RESTART_CHECK_DELAY)
        log('[WARNING] communication session recovery requested')
        
    async def session():
        nonlocal soc, ADDON_STARTED
        if REBOOT_CONTROL:
            # Async wait: shutdown and process heartbeat stay responsive.
            deadline = time.monotonic() + MQTT_RECONNECT_TIMEOUT
            while not MQTT_ONLINE:
                if time.monotonic() > deadline:
                    raise TimeoutError('HA MQTT birth message timeout')
                await asyncio.sleep(0.2)
        if comm_mode in ('socket', 'mixed'):
            soc = await initiate_socket()
        await asyncio.sleep(startup_delay)
        ADDON_STARTED = True
        log('[INFO] 장치 등록 및 상태 업데이트를 시작합니다')
        tasklist = [asyncio.create_task(coro) for coro in (
            state_update_loop(), command_loop(), ew11_health_loop(),
            addon_health_loop(), mqtt_health_loop(), diagnostic_heartbeat_loop(),
        )]
        if comm_mode == 'socket':
            tasklist.append(asyncio.create_task(serial_recv_loop()))
        recovery = asyncio.create_task(restart_control())
        try:
            done, _ = await asyncio.wait(tasklist + [recovery], return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                if task is not recovery:
                    task.result()  # Full traceback is logged by the session owner.
                    raise RuntimeError('communication task unexpectedly returned')
        finally:
            await cancel_tasks(tasklist + [recovery])

    previous_exception_handler = session_loop.get_exception_handler()
    def asyncio_exception_handler(loop, context):
        nonlocal restart_flag
        error = context.get('exception')
        detail = ''.join(traceback.format_exception(type(error), error, error.__traceback__)) if error else context.get('message')
        log('[ERROR] asyncio unhandled failure: {}'.format(detail))
        restart_flag = True
    session_loop.set_exception_handler(asyncio_exception_handler)
    for sig in (signal.SIGTERM, signal.SIGINT):
        session_loop.add_signal_handler(sig, shutdown.set)
    heartbeat = asyncio.create_task(heartbeat_loop())
    stop_task = asyncio.create_task(shutdown.wait())
    delay = 1
    try:
        while not shutdown.is_set():
            restart_flag = False
            MQTT_ONLINE = False
            ADDON_STARTED = False
            mqtt_connected = False
            mqtt_disconnected_time = time.monotonic()
            last_command_time = last_state_update_time = last_valid_packet_time = time.monotonic()
            MSG_QUEUE = Queue(maxsize=1000)
            CMD_QUEUE = asyncio.Queue(maxsize=100)
            DEVICE_STATE, MSG_CACHE, DISCOVERY_LIST, RESIDUE = {}, {}, [], ''
            force_target_time = time.monotonic() + FORCE_PERIOD
            force_stop_time = force_target_time + FORCE_DURATION
            FORCE_UPDATE = False
            startup_delay = 0
            # A fresh client includes a fresh socket, reconnect state and network thread.
            mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='mqtt-ezville')
            mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
            mqtt_client.on_connect = on_connect
            mqtt_client.on_disconnect = on_disconnect
            mqtt_client.on_message = on_message
            mqtt_client.on_log = on_mqtt_log
            mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
            started = time.monotonic()
            active = None
            try:
                mqtt_client.connect_async(config['mqtt_server'], port=config.get('mqtt_port', 1883))
                mqtt_client.loop_start()
                active = asyncio.create_task(session())
                done, _ = await asyncio.wait([active, stop_task, heartbeat], return_when=asyncio.FIRST_COMPLETED)
                if heartbeat in done:
                    heartbeat.result()
                    raise RuntimeError('process heartbeat stopped')
                if active in done:
                    active.result()
            except Exception:
                log('[ERROR] communication session failed:\n{}'.format(traceback.format_exc()))
            finally:
                if active is not None:
                    await cancel_tasks([active])
                ADDON_STARTED = False
                if soc is not None:
                    soc.close()
                    soc = None
                client = mqtt_client
                def close_mqtt():
                    client.disconnect()
                    client.loop_stop()
                # Cleanup failure escapes this loop. Parent restarts the worker.
                await bounded_call(close_mqtt, 10)
            if shutdown.is_set():
                break
            restart_count += 1
            if time.monotonic() - started >= 300:
                delay = 1
            log('[WARNING] session restart count={}, retry in {}s'.format(restart_count, delay))
            try:
                await asyncio.wait_for(shutdown.wait(), delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2, 60)
    finally:
        await cancel_tasks([heartbeat, stop_task])
        for sig in (signal.SIGTERM, signal.SIGINT):
            session_loop.remove_signal_handler(sig)
        session_loop.set_exception_handler(previous_exception_handler)
        log('[INFO] communication worker stopped')


if __name__ == '__main__':
    with open(config_dir + '/options.json') as file:
        CONFIG = json.load(file)

    init_diagnostic_logging(CONFIG)
    # SIGUSR1 is sent by the independent parent before terminating a stalled worker.
    stack_path = CONFIG.get('diagnostic_log_file', '/share/simple_mqtt_ezville_control.log') + '.stacks.log'
    try:
        if os.path.exists(stack_path) and os.path.getsize(stack_path) > 5 * 1024 * 1024:
            os.replace(stack_path, stack_path + '.1')
        stack_file = open(stack_path, 'a')
        stack_file.write('\nWorker pid={} started {}\n'.format(os.getpid(), time.strftime('%Y-%m-%d %H:%M:%S')))
        stack_file.flush()
    except OSError:
        stack_file = sys.stderr
    faulthandler.register(signal.SIGUSR1, file=stack_file, all_threads=True)

    def thread_exception_handler(args):
        detail = ''.join(traceback.format_exception(
            args.exc_type, args.exc_value, args.exc_traceback
        ))
        log('[FATAL] Thread 비정상 종료 ({}):\n{}'.format(args.thread.name, detail))

    threading.excepthook = thread_exception_handler
    log('[START] addon process 시작: mode={}, pid={}, Python={}'.format(
        CONFIG.get('mode'), os.getpid(), sys.version.split()[0]))
    log('[CONFIG] {}'.format(json.dumps({
        key: CONFIG.get(key) for key in (
            'mode', 'ew11_port', 'command_interval', 'command_retry_count',
            'state_loop_delay', 'command_loop_delay', 'serial_recv_delay',
            'restart_check_delay', 'reboot_control', 'ew11_buffer_size',
            'ew11_timeout', 'diagnostic_interval'
        )
    }, ensure_ascii=False, sort_keys=True)))
    try:
        asyncio.run(ezville_loop(CONFIG))
    except BaseException:
        log('[FATAL] addon process 비정상 종료:\n{}'.format(traceback.format_exc()))
        raise

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
from queue import Queue

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

    directory = os.path.dirname(log_file)
    if directory:
        os.makedirs(directory, exist_ok=True)

    handler = TimedRotatingFileHandler(
        log_file, when='midnight', backupCount=backup_count, encoding='utf-8'
    )
    handler.setFormatter(logging.Formatter('%(message)s'))
    handler.suffix = '%Y-%m-%d'
    diagnostic_logger.handlers.clear()
    diagnostic_logger.addHandler(handler)


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
def ezville_loop(config):
    
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
    MSG_QUEUE = Queue()
    
    # EW11에 보낼 Command 및 예상 Acknowledge 패킷 
    CMD_QUEUE = asyncio.Queue()
    
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
    last_received_time = time.time()

    # Paho의 자동 재접속 스레드까지 멈춘 경우를 감지하기 위한 상태값
    MQTT_RECONNECT_TIMEOUT = 120
    mqtt_connected = False
    mqtt_disconnected_time = time.monotonic()
    last_mqtt_message_time = time.time()
    diagnostic_interval = config.get('diagnostic_interval', 600)
    restart_count = 0
    
    # Addon 먹통 상태 감지 및 자체 리셋 설정
    last_command_time = time.time()
    last_state_update_time = time.time()
    HEALTH_CHECK_INTERVAL = 60  # 60초마다 헬스 체크 실행
    HEALTH_CHECK_TIMEOUT = 300  # 300초(5분) 동안 활동이 없으면 먹통으로 판단
    
    # EW11 재시작 확인용 Flag
    restart_flag = False
  
    # MQTT Integration 활성화 확인 Flag - 단, 사용을 위해서는 MQTT Integration에서 Birth/Last Will Testament 설정 및 Retain 설정 필요
    MQTT_ONLINE = False
    
    # Addon 정상 시작 Flag
    ADDON_STARTED = False
 
    # Reboot 이후 안정적인 동작을 위한 제어 Flag
    REBOOT_CONTROL = config['reboot_control']
    REBOOT_DELAY = config['reboot_delay']

    # 시작 시 인위적인 Delay 필요시 사용
    startup_delay = 0
  

    # MQTT 통신 연결 Callback
    def on_connect(client, userdata, flags, rc):
        nonlocal mqtt_connected
        nonlocal mqtt_disconnected_time

        if rc == 0:
            mqtt_connected = True
            mqtt_disconnected_time = None
            log('[INFO] MQTT Broker 연결 성공')
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
            errcode = {1: 'Connection refused - incorrect protocol version',
                       2: 'Connection refused - invalid client identifier',
                       3: 'Connection refused - server unavailable',
                       4: 'Connection refused - bad username or password',
                       5: 'Connection refused - not authorised'}
            log(errcode[rc])
         
        
    # MQTT 메시지 Callback
    def on_message(client, userdata, msg):
        nonlocal MSG_QUEUE
        nonlocal MQTT_ONLINE
        nonlocal startup_delay
        nonlocal last_mqtt_message_time

        last_mqtt_message_time = time.time()
        
        if msg.topic == 'homeassistant/status':
            # Reboot Control 사용 시 MQTT Integration의 Birth/Last Will Testament Topic은 바로 처리
            if REBOOT_CONTROL:
                status = msg.payload.decode('utf-8')
                
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
        else:
            MSG_QUEUE.put(msg)
 

    # MQTT 통신 연결 해제 Callback
    def on_disconnect(client, userdata, rc):
        nonlocal mqtt_connected
        nonlocal mqtt_disconnected_time

        mqtt_connected = False
        if mqtt_disconnected_time is None:
            mqtt_disconnected_time = time.monotonic()
        log('[WARNING] MQTT 연결 해제 (rc={})'.format(rc))

    def on_mqtt_log(client, userdata, level, buf):
        # Paho 내부 경고/오류만 보존한다. 정상 패킷 debug는 장기 로그를
        # 과도하게 키우므로 MQTT_LOG를 켠 경우에만 기록한다.
        if level in (mqtt.MQTT_LOG_WARNING, mqtt.MQTT_LOG_ERR) or mqtt_log:
            log('[MQTT] level={} {}'.format(level, buf))


    # MQTT message를 분류하여 처리
    async def process_message():
        # MSG_QUEUE의 message를 하나씩 pop
        nonlocal MSG_QUEUE
        nonlocal last_received_time
        
        stop = False
        while not stop:
            if MSG_QUEUE.empty():
                stop = True
            else:
                msg = MSG_QUEUE.get()
                topics = msg.topic.split('/')

                if topics[0] == HA_TOPIC and topics[-1] == 'command':
                    await HA_process(topics, msg.payload.decode('utf-8'))
                elif topics[0] == EW11_TOPIC and topics[-1] == 'recv':
                    # Que에서 확인된 시간 기준으로 EW11 Health Check함.
                    last_received_time = time.time()

                    await EW11_process(msg.payload.hex().upper())
                   
    
    # EW11 전달된 메시지 처리
    async def EW11_process(raw_data):
        nonlocal DISCOVERY_LIST
        nonlocal RESIDUE
        nonlocal MSG_CACHE
        nonlocal DEVICE_STATE
        nonlocal last_state_update_time
        
        raw_data = RESIDUE + raw_data
        last_state_update_time = time.time()
        
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
                                        await update_state(name, 'auto', rid, id, onoff)
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
        mqtt_client.publish(topic, json.dumps(payload))

    
    # 장치 State를 MQTT로 Publish
    async def update_state(device, state, id1, id2, value):
        nonlocal DEVICE_STATE

        deviceID = '{}_{:0>2d}_{:0>2d}'.format(device, id1, id2)
        key = deviceID + state
        
        if value != DEVICE_STATE.get(key) or FORCE_UPDATE:
            DEVICE_STATE[key] = value
            
            topic = STATE_TOPIC.format(deviceID, state)
            mqtt_client.publish(topic, value.encode())
                    
            if mqtt_log:
                log('[LOG] ->> HA : {} >> {}'.format(topic, value))

        return

    
    # HA에서 전달된 메시지 처리        
    async def HA_process(topics, value):
        nonlocal CMD_QUEUE
        nonlocal last_command_time

        device_info = topics[1].split('_')
        device = device_info[0]
        last_command_time = time.time()
        
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
                           
                            await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                        
                        # Thermostat는 외출 모드를 Off 모드로 연결
                        elif value == 'off':
 
                            sendcmd = checksum('F7' + RS485_DEVICE[device]['away']['id'] + '1' + str(idx) + RS485_DEVICE[device]['away']['cmd'] + '01010000')
                            recvcmd = 'F7' + RS485_DEVICE[device]['away']['id'] + '1' + str(idx) + RS485_DEVICE[device]['away']['ack']
                            statcmd = [key, value]
                           
                            await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                        
#                        elif value == 'off':
#                        
#                            sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '01000000')
#                            recvcmd = 'F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']
#                            statcmd = [key, value]
#                           
#                            await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})                    
                                               
                        if debug:
                            log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                                    
                    elif topics[2] == 'setTemp':                            
                        value = int(float(value))
   
                        sendcmd = checksum('F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['cmd'] + '01' + "{:02X}".format(value) + '0000')
                        recvcmd = 'F7' + RS485_DEVICE[device]['target']['id'] + '1' + str(idx) + RS485_DEVICE[device]['target']['ack']
                        statcmd = [key, str(value)]

                        await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
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
                    
                    await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
                    if debug:
                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                                
                elif device == 'plug':                         
                    pwr = '01' if value == 'ON' else '00'

                    sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '020' + str(sid) + pwr + '0000')
                    recvcmd = 'F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']
                    statcmd = [key, value]
                        
                    await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
                    if debug:
                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
                                
                elif device == 'gasvalve':
                    # 가스 밸브는 ON 제어를 받지 않음
                    if value == 'OFF':
                        sendcmd = checksum('F7' + RS485_DEVICE[device]['power']['id'] + '0' + str(idx) + RS485_DEVICE[device]['power']['cmd'] + '0100' + '0000')
                        recvcmd = ['F7' + RS485_DEVICE[device]['power']['id'] + '1' + str(idx) + RS485_DEVICE[device]['power']['ack']]
                        statcmd = [key, value]

                        await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                               
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
                    
                    await CMD_QUEUE.put({'sendcmd': sendcmd, 'recvcmd': recvcmd, 'statcmd': statcmd})
                    
                    if debug:
                        log('[DEBUG] Queued ::: sendcmd: {}, recvcmd: {}, statcmd: {}'.format(sendcmd, recvcmd, statcmd))
  
                                                
    # HA에서 전달된 명령을 EW11 패킷으로 전송
    async def send_to_ew11(send_data):
            
        for i in range(CMD_RETRY_COUNT):
            if ew11_log:
                log('[SIGNAL] 신호 전송: {}'.format(send_data))
                        
            if comm_mode == 'mqtt':
                mqtt_client.publish(EW11_SEND_TOPIC, bytes.fromhex(send_data['sendcmd']))
            else:
                nonlocal soc
                try:
                    soc.sendall(bytes.fromhex(send_data['sendcmd']))
                except OSError:
                    soc.close()
                    soc = initiate_socket()
                    soc.sendall(bytes.fromhex(send_data['sendcmd']))
            if debug:                     
                log('[DEBUG] Iter. No.: ' + str(i + 1) + ', Target: ' + send_data['statcmd'][1] + ', Current: ' + DEVICE_STATE.get(send_data['statcmd'][0]))
             
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

        if ew11_log:
            log('[SIGNAL] {}회 명령을 재전송하였으나 수행에 실패했습니다.. 다음의 Queue 삭제: {}'.format(str(CMD_RETRY_COUNT),send_data))
            return
        
                                                
    # EW11 동작 상태를 체크해서 필요시 리셋 실시 및 HA에 상태 알림
    async def ew11_health_loop():
        nonlocal restart_flag
        
        ew11_status = 'online'  # EW11 상태 추적: 'online' 또는 'offline'
        
        while True:
            timestamp = time.time()
            time_since_last_packet = timestamp - last_received_time
        
            # TIMEOUT 시간 동안 새로 받은 EW11 패킷이 없으면 상태를 'offline'으로 변경 및 HA에 알림
            if time_since_last_packet > EW11_TIMEOUT:
                if ew11_status == 'online':
                    # 처음 타임아웃 감지 시 HA에 알림
                    ew11_status = 'offline'
                    log('[WARNING] EW11 패킷 수신 타임아웃: {:.1f}초 동안 신호 없음'.format(time_since_last_packet))
                    
                    # HA에 EW11 상태 알림 (MQTT publish)
                    try:
                        payload = {'status': 'offline', 'timestamp': int(time.time()), 'last_packet_time': int(last_received_time)}
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
        ew11_id = config['ew11_id']
        ew11_password = config['ew11_password']
        ew11_server = config['ew11_server']

        # timeout 없는 Telnet 호출은 EW11 장애 시 복구 루프도 영구 정지시킨다.
        ew11 = telnetlib.Telnet(ew11_server, timeout=10)
        try:
            ew11.read_until(b'login:', timeout=5)
            ew11.write(ew11_id.encode('utf-8') + b'\n')
            ew11.read_until(b'password:', timeout=5)
            ew11.write(ew11_password.encode('utf-8') + b'\n')
            ew11.write(b'Restart\n')
            ew11.read_until(b'Restart..', timeout=5)
        finally:
            ew11.close()
        
        log('[INFO] EW11 리셋 완료')
        
        # 리셋 후 60초간 Delay
        await asyncio.sleep(60)
        
    # Addon 먹통 상태 감지 및 자동 리셋
    async def addon_health_loop():
        nonlocal restart_flag
        
        while True:
            timestamp = time.time()
            
            # 마지막 명령 또는 상태 업데이트 시간 확인
            time_since_command = timestamp - last_command_time
            time_since_state = timestamp - last_state_update_time
            
            # 명령과 상태 업데이트 중 더 최근의 시간 기준으로 판단
            last_activity_time = max(last_command_time, last_state_update_time)
            time_since_activity = timestamp - last_activity_time
            
            if time_since_activity > HEALTH_CHECK_TIMEOUT:
                log('[WARNING] Addon 먹통 감지: {}초 동안 활동 없음. 자동 리셋 트리거'.format(int(time_since_activity)))
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
            now = time.time()
            log('[HEALTH] mqtt_connected={} mqtt_message_age={:.1f}s '
                'ew11_packet_age={:.1f}s state_age={:.1f}s command_age={:.1f}s '
                'msg_queue={} cmd_queue={} restart_flag={} restart_count={}'.format(
                    mqtt_connected,
                    now - last_mqtt_message_time,
                    now - last_received_time,
                    now - last_state_update_time,
                    now - last_command_time,
                    MSG_QUEUE.qsize(),
                    CMD_QUEUE.qsize(),
                    restart_flag,
                    restart_count))
            await asyncio.sleep(diagnostic_interval)
    
    def initiate_socket():
        # SOCKET 통신 시작
        log('[INFO] Socket 연결을 시작합니다')
            
        retry_count = 0
        while True:
            try:
                soc = socket.socket()
                soc.settimeout(10)
                soc.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                connect_socket(soc)
                soc.settimeout(None)
                return soc
            except OSError as e:
                soc.close()
                retry_count += 1
                log('[ERROR] Socket 연결 실패: {} ({}회 재시도)'.format(e, retry_count))
                if retry_count >= 3:
                    raise
                time.sleep(1)
             
            
    def connect_socket(socket):
        socket.connect((SOC_ADDRESS, SOC_PORT))
    

    async def serial_recv_loop():
        nonlocal soc
        nonlocal MSG_QUEUE
        
        class MSG:
            topic = ''
            payload = bytearray()
        
        msg = MSG()
        
        while True:
            try:
                # EW11 버퍼 크기만큼 데이터 받기
                DATA = soc.recv(EW11_BUFFER_SIZE)
                msg.topic = EW11_TOPIC + '/recv'
                msg.payload = DATA   
                
                MSG_QUEUE.put(msg)
                
            except OSError:
                soc.close()
                soc = initiate_socket()
         
            await asyncio.sleep(SERIAL_RECV_DELAY) 
        
        
    async def state_update_loop():
        nonlocal force_target_time
        nonlocal force_stop_time
        nonlocal FORCE_UPDATE
        
        while True:
            await process_message()                    
            
            timestamp = time.time()
            
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
 

    # EW11 재실행 시 리스타트 실시
    async def restart_control():
        nonlocal mqtt_client
        nonlocal restart_flag
        nonlocal MQTT_ONLINE
        nonlocal restart_count
        
        while True:
            if restart_flag or (not MQTT_ONLINE and ADDON_STARTED and REBOOT_CONTROL):
                if restart_flag:
                    log('[WARNING] EW11 재시작 확인')
                elif not MQTT_ONLINE and ADDON_STARTED and REBOOT_CONTROL:
                    log('[WARNING] 동작 중 MQTT Integration Offline 변경')
                
                # Asyncio Loop 획득
                loop = asyncio.get_event_loop()
                
                # MTTQ 및 socket 연결 종료
                log('[WARNING] 모든 통신 종료')
                mqtt_client.loop_stop()
                if comm_mode == 'mixed' or comm_mode == 'socket':
                    nonlocal soc
                    soc.close()
                       
                # flag 원복
                restart_count += 1
                restart_flag = False
                MQTT_ONLINE = False

                # asyncio loop 종료
                log('[WARNING] asyncio loop 종료')
                loop.stop()
            
            # RESTART_CHECK_DELAY초 마다 실행
            await asyncio.sleep(RESTART_CHECK_DELAY)

        
    # MQTT 통신
    # mqtt_client = mqtt.Client('mqtt-ezville')
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, 'mqtt-ezville')
    mqtt_client.username_pw_set(config['mqtt_id'], config['mqtt_password'])
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message
    mqtt_client.on_log = on_mqtt_log
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)
    mqtt_client.connect_async(config['mqtt_server'])
    
    # asyncio loop 획득 및 EW11 오류시 재시작 task 등록
    loop = asyncio.get_event_loop()

    def asyncio_exception_handler(loop, context):
        error = context.get('exception')
        detail = ''.join(traceback.format_exception(type(error), error, error.__traceback__)) if error else context.get('message')
        log('[ERROR] asyncio 미처리 예외: {}'.format(detail))

    loop.set_exception_handler(asyncio_exception_handler)
    loop.create_task(restart_control())
        
    # Discovery 및 강제 업데이트 시간 설정
    force_target_time = time.time() + FORCE_PERIOD
    force_stop_time = force_target_time + FORCE_DURATION
    

    while True:
        # MQTT 통신 시작
        mqtt_client.loop_start()
        # MQTT Integration의 Birth/Last Will Testament를 기다림 (1초 단위)
        while not MQTT_ONLINE and REBOOT_CONTROL:
            log('[INFO] Waiting for MQTT connection')
            time.sleep(1)
        
        # socket 통신 시작       
        if comm_mode == 'mixed' or comm_mode == 'socket':
            soc = initiate_socket()  

        log('[INFO] 장치 등록 및 상태 업데이트를 시작합니다')

        tasklist = []
 
        # 필요시 Discovery 등의 지연을 위해 Delay 부여 
        time.sleep(startup_delay)      
  
        # socket 데이터 수신 loop 실행
        if comm_mode == 'socket':
            tasklist.append(loop.create_task(serial_recv_loop()))
        # EW11 패킷 기반 state 업데이트 loop 실행
        tasklist.append(loop.create_task(state_update_loop()))
        # Home Assistant 명령 실행 loop 실행
        tasklist.append(loop.create_task(command_loop()))
        # EW11 상태 체크 loop 실행
        tasklist.append(loop.create_task(ew11_health_loop()))
        # Addon 먹통 상태 감지 및 자동 리셋 loop 실행
        tasklist.append(loop.create_task(addon_health_loop()))
        # MQTT 자동 재접속 자체가 멈춘 경우 감지
        tasklist.append(loop.create_task(mqtt_health_loop()))
        # 장기 장애 분석용 상태 스냅샷 기록
        tasklist.append(loop.create_task(diagnostic_heartbeat_loop()))

        # create_task의 예외는 await하지 않으면 통신 task만 조용히 죽을 수
        # 있다. 즉시 기록하고 기존 restart_control을 통해 전체를 재생성한다.
        def task_done(task):
            nonlocal restart_flag
            if task.cancelled():
                return
            error = task.exception()
            if error is not None:
                detail = ''.join(traceback.format_exception(type(error), error, error.__traceback__))
                log('[ERROR] 백그라운드 Task 비정상 종료:\n{}'.format(detail))
                restart_flag = True

        for task in tasklist:
            task.add_done_callback(task_done)
        
        # ADDON 정상 시작 Flag 설정
        ADDON_STARTED = True
        loop.run_forever()
        
        # 이전 task는 취소
        log('[INFO] 이전 실행 Task 종료')
        for task in tasklist:
            task.cancel()

        ADDON_STARTED = False
        
        # 주요 변수 초기화    
        MSG_QUEUE = Queue()
        CMD_QUEUE = asyncio.Queue()
        DEVICE_STATE = {}
        MSG_CACHE = {}
        DISCOVERY_LIST = []
        RESIDUE = ''
        last_command_time = time.time()
        last_state_update_time = time.time()


if __name__ == '__main__':
    with open(config_dir + '/options.json') as file:
        CONFIG = json.load(file)

    init_diagnostic_logging(CONFIG)

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
        ezville_loop(CONFIG)
    except BaseException:
        log('[FATAL] addon process 비정상 종료:\n{}'.format(traceback.format_exc()))
        raise

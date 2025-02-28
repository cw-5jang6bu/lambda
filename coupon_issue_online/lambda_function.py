import redis
import json
import boto3
import uuid
import time
from datetime import datetime, timedelta
from rediscluster import RedisCluster 
import pytz

# Redis 클러스터 엔드포인트 설정 
redis_host = ""
redis_port = 6379  # 기본 포트

def get_current_timestamp(timezone=None):
    """ 현재 시간을 타임존을 반영하여 ISO 8601 형식으로 반환 """
    if timezone:
        try:
            tz = pytz.timezone(timezone)
        except pytz.UnknownTimeZoneError:
            tz = pytz.timezone('Asia/Seoul')  # 잘못된 값이 들어오면 기본값
    else:
        tz = pytz.timezone('Asia/Seoul')

    now = datetime.now(tz)
    return now.isoformat()  # ISO 8601 형식으로 반환 (예: 2025-02-24T10:00:00+09:00)

def get_expiry_timestamp_for_today(timezone):
    # 기본 타임존을 'Asia/Seoul'로 설정
    if timezone:
        try:
            tz = pytz.timezone(timezone)
        except pytz.UnknownTimeZoneError:
            tz = pytz.timezone('Asia/Seoul')  # 잘못된 값이 들어오면 기본값으로 설정
    else:
        tz = pytz.timezone('Asia/Seoul')

    # 현재 시간 기준의 자정(23:59:59) 계산
    now = datetime.now(tz)
    midnight = datetime.combine(now.date(), datetime.min.time()) + timedelta(days=1)
    midnight = tz.localize(midnight)  # 타임존 적용
    expiry_timestamp = int(midnight.timestamp()) - 1  # 23:59:59 설정

    return expiry_timestamp

def get_redis_client():
    # RedisCluster 클라이언트로 연결
    redis_client = RedisCluster(
        host=redis_host,
        port=redis_port,
        ssl=True, 
        skip_full_coverage_check=True
    )
    return redis_client

def has_received_coupon(redis_client, member_id):
    # 사용자가 이미 쿠폰을 받은 적 있는지 확인
    return redis_client.sismember("received_coupons", member_id)

def check_and_decrement_coupons(redis_client, coupon_key, member_id, timezone):
    # 쿠폰 개수를 확인하고 1개 차감
    remaining_coupons = redis_client.get(coupon_key)
    if remaining_coupons and int(remaining_coupons) > 0:
        redis_client.decrby(coupon_key, 1)  # 원자적 감소
        redis_client.sadd("received_coupons", member_id)  # 중복 체크를 위한 발급된 사용자 저장
        
        # 쿠폰 ID 생성
        coupon_id = f"{coupon_key}-{str(uuid.uuid4())}"

        # 만료 시간 계산 (오늘 자정 00:00)
        expiry_timestamp = get_expiry_timestamp_for_today(timezone)

        # 생성 시간 계산
        issued_at = get_current_timestamp(timezone)

        # Redis에 쿠폰 정보 저장 
        coupon_data = {
            "member_id": member_id, 
            "used": False, 
            "issued_at": issued_at, 
            "timezone": timezone
        }
        redis_client.set(f"coupon:{coupon_id}", json.dumps(coupon_data))
        redis_client.expireat(f"coupon:{coupon_id}", expiry_timestamp)  
        redis_client.hset("member_coupons", member_id, coupon_id)
        return coupon_id
    return None

def process_sqs_message(message_body):
    # SQS 메시지를 파싱하고 쿠폰을 발급
    redis_client = get_redis_client()

    # SQS에서 받은 메시지를 JSON으로 변환
    message = json.loads(message_body)
    member_id = message.get("member_id")
    timezone = message.get("timezone")

    # 로그인 확인
    if not member_id:
        return {"statusCode": 400, "body": "Invalid request: missing member_id"}

    # 쿠폰 중복 발급 방지
    if has_received_coupon(redis_client, member_id):
        return {"statusCode": 400, "body": "User has already received a coupon"}

    # 오프라인 쿠폰 발급
    coupon_id = check_and_decrement_coupons(redis_client, "online", member_id, timezone)
    if coupon_id:
        return {"statusCode": 200, "body": f"online coupon granted successfully. Coupon ID: {coupon_id}"}
    else:
        return {"statusCode": 400, "body": "No online coupons remaining"}

def lambda_handler(event, context):
    print("Lambda 실행 - SQS 메시지 처리 시작")

    for record in event.get('Records', []):
        receipt_handle = record['receiptHandle']  # 메시지 삭제를 위한 핸들
        message_body = record['body']  # 메시지 본문

        response = process_sqs_message(message_body)
        print(f"process_sqs_message response: {response}")

    return response

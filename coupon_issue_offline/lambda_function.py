import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), 'package'))
import redis
import json
import boto3
import uuid
import time
from datetime import datetime, timedelta

# Redis 클러스터 엔드포인트 설정 
# TODO: 실제 엔드포인트로 변경해야함
redis_host = "redis-cluster.lbkn6z.clustercfg.apn2.cache.amazonaws.com"
redis_port = 6379  # 기본 포트

# AWS SQS 클라이언트 설정
sqs = boto3.client('sqs')
# SQS 큐 URL (API Gateway에서 요청을 받아 저장하는 큐)
# TODO: 실제 엔드포인트로 변경해야함
SQS_QUEUE_URL = "https://sqs.ap-northeast-2.amazonaws.com/123456789012/offline-coupon-queue"

# ✅ Connection Pool을 사용하여 Redis 연결을 재사용
redis_pool = redis.ConnectionPool(
    host=redis_host,
    port=redis_port,
    decode_responses=True
)

def get_expiry_timestamp_for_today():
    # 오늘 자정 00:00의 UNIX 타임스탬프 계산
    now = datetime.now()
    midnight = datetime.combine(now.date(), datetime.min.time()) + timedelta(days=1)  # 내일 자정
    expiry_timestamp = int(time.mktime(midnight.timetuple())) - 1  # 23:59:59로 설정
    return expiry_timestamp

def get_redis_client():
    return redis.Redis(connection_pool=redis_pool)

def has_received_coupon(redis_client, user_id):
    # 사용자가 이미 쿠폰을 받은 적 있는지 확인
    return redis_client.sismember("received_coupons", user_id)

def check_and_decrement_coupons(redis_client, coupon_key, user_id):
    # 쿠폰 개수를 확인하고 1개 차감
    remaining_coupons = redis_client.get(coupon_key)
    if remaining_coupons and int(remaining_coupons) > 0:
        redis_client.decrby(coupon_key, 1)  # 원자적 감소
        redis_client.sadd("received_coupons", user_id)  # 중복 체크를 위한 발급된 사용자 저장
        
        # 쿠폰 ID 생성
        coupon_id = f"{coupon_key}-{str(uuid.uuid4())}"

        # 사용자가 발급받은 쿠폰 저장 (user_id → coupon_id 매핑)
        # TODO: 쿠폰키는 저장안하나? 해야됨
        redis_client.hset("user_coupons", user_id, coupon_id)

        # 만료 시간 계산 (오늘 자정 00:00)
        expiry_timestamp = get_expiry_timestamp_for_today()

        # Redis에 쿠폰 정보 저장 (만료시간 설정 가능)
        coupon_data = {"user_id": user_id, "used": False}
        redis_client.set(f"coupon:{coupon_id}", json.dumps(coupon_data))
        redis_client.expireat(f"coupon:{coupon_id}", expiry_timestamp)  # 쿠폰 만료 설정
        return coupon_id
    return None

def process_sqs_message(message_body):
    # SQS 메시지를 파싱하고 쿠폰을 발급
    redis_client = get_redis_client()

    # SQS에서 받은 메시지를 JSON으로 변환
    message = json.loads(message_body)
    user_id = message.get("user_id")

    # 로그인 확인
    if not user_id:
        return {"statusCode": 400, "body": "Invalid request: missing user_id"}

    # 쿠폰 중복 발급 방지
    if has_received_coupon(redis_client, user_id):
        return {"statusCode": 400, "body": "User has already received a coupon"}

    # 오프라인 쿠폰 발급
    coupon_id = check_and_decrement_coupons(redis_client, "offline", user_id)
    if coupon_id:
        return {"statusCode": 200, "body": f"Offline coupon granted successfully. Coupon ID: {coupon_id}"}
    else:
        return {"statusCode": 400, "body": "No offline coupons remaining"}

def lambda_handler(event, context):
    """SQS에서 메시지를 가져와 처리"""
    print("Lambda 실행 - SQS 메시지 처리 시작")

    for record in event.get('Records', []):
        receipt_handle = record['receiptHandle']  # 메시지 삭제를 위한 핸들
        message_body = record['body']  # 메시지 본문

        response = process_sqs_message(message_body)

        # 처리 완료된 메시지는 SQS에서 삭제
        if response["statusCode"] == 200:
            sqs.delete_message(QueueUrl=SQS_QUEUE_URL, ReceiptHandle=receipt_handle)
            print(f"Processed and deleted message: {message_body}")
        else:
            print(f"Failed to process message: {message_body}")

    return {"statusCode": 200, "body": "Lambda execution completed"}
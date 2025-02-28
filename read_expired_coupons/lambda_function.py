import logging
import boto3
import json
import redis
from rediscluster import RedisCluster  # Redis 클러스터 사용

# 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Redis 클러스터 엔드포인트 설정
redis_host = ""
redis_port = 6379

# SQS 클라이언트 설정
sqs_client = boto3.client('sqs')

# SQS 큐 URL 
sqs_queue_url = 'https://sqs.ap-northeast-2.amazonaws.com/034362047320/coupon-redis-to-aurora-queue.fifo'

def get_redis_client():
    """Redis 클러스터 연결"""
    redis_client = RedisCluster(
        host=redis_host,
        port=redis_port,
        ssl=True,  # TLS 연결 활성화
        ssl_cert_reqs=None,
        skip_full_coverage_check=True
    )
    return redis_client


def send_sqs_message(payload):
    """
    SQS 메시지 전송
    """
    try:
        message_body = json.dumps(payload)

        logger.info(f"MessageBody to be sent: {message_body}")  # MessageBody 값 출력

        # SQS 메시지 전송
        sqs_response = sqs_client.send_message(
            QueueUrl=sqs_queue_url,
            MessageBody=message_body,
            MessageGroupId='default',  # FIFO 큐일 경우 필수
            MessageDeduplicationId=payload['coupon_id']  # 중복 메시지를 피하기 위한 고유 ID
        )
        print(f"SQS Response: {sqs_response}")
        logger.info(f"SQS message sent for coupon {payload['coupon_id']}, SQS Response: {sqs_response}")
    except Exception as e:
        logger.error(f"Error sending message to SQS: {e}")

    
def lambda_handler(event, context):
    """
    Lambda 함수 핸들러. 1시간마다 Redis에서 데이터를 읽어 eventbridge_expired_coupons_lambda로 전달.
    """
    # Redis 클라이언트 생성
    redis_client = get_redis_client()

    # Redis에서 모든 쿠폰 정보를 가져옴
    coupon_keys = redis_client.keys('coupon:offline-*') + redis_client.keys('coupon:online-*')

    if not coupon_keys:
        print("No coupons found.")
        return

    for coupon_key in coupon_keys:
        coupon_data = redis_client.get(coupon_key)  # 쿠폰 데이터를 가져옴

        if coupon_data:
            try:
                coupon_info = json.loads(coupon_data)  # JSON 파싱
                # print(f"Parsed coupon info: {coupon_info}")  # 파싱된 쿠폰 정보 출력
                
                # 쿠폰 ID는 'coupon:' 접두사를 제거하여 처리
                coupon_id = coupon_key.decode().replace('coupon:', '', 1)
                # 필요한 페이로드 형식으로 정보 구성
                payload = {
                    'coupon_id': coupon_id,  # 쿠폰 ID
                    'member_id': coupon_info.get('member_id', ''),  # 회원 ID
                    'timezone': coupon_info.get('timezone', ''),  # 시간대
                    'used': coupon_info.get('used', ''),  # 사용 여부
                    'issued_at': coupon_info.get('issued_at', '')  # 발급일
                }
                # Lambda 호출 전에 페이로드 출력
                print(f"Payload to Lambda: {payload}")
                
                # SQS로 메시지 전송
                send_sqs_message(payload)

            except json.JSONDecodeError as e:
                print(f"JSON decode error for {coupon_key}: {e}")
        else:
            print(f"Coupon data not found for {coupon_key}")

    return {
        'statusCode': 200,
        'body': json.dumps('All coupons processed and sent')
    }
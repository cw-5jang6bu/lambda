import json
import pytz
import logging
from rediscluster import RedisCluster
import pymysql

# 로거 설정
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Redis 클러스터 엔드포인트 설정 
redis_host = "REDIS_HOST"
redis_port = 6379

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

# Aurora MySQL 연결 설정
rds_host = 'RDS_HOST'
username = 'admin'
password = 'password'
database = 'event_db'

def get_db_connection():
    """Aurora MySQL 연결"""
    connection = pymysql.connect(host=rds_host, user=username, password=password, database=database)
    return connection

def process_sqs_message(message_body):
    """SQS 메시지를 파싱하고 쿠폰을 발급"""
    redis_client = get_redis_client()

    # SQS에서 받은 메시지를 JSON으로 변환
    try:
        message = json.loads(message_body)
    except json.JSONDecodeError:
        logger.error(f"Failed to decode JSON message: {message_body}")
        return {"statusCode": 400, "body": json.dumps("Invalid JSON format.")}

    member_id = message.get("member_id")
    timezone_str = message.get("timezone", 'Asia/Seoul')  # 기본 시간대 설정

    # 시간대 검증
    try:
        tz = pytz.timezone(timezone_str)
    except pytz.UnknownTimeZoneError:
        logger.error(f"Invalid timezone: {timezone_str}")
        return {"statusCode": 400, "body": json.dumps(f"Invalid timezone: {timezone_str}")}

    coupon_id = message.get("coupon_id")

    # Redis에서 쿠폰 정보 조회
    coupon_info = redis_client.hgetall(f"coupon:{coupon_id}")
    if not coupon_info:
        logger.warning(f"Coupon {coupon_id} not found in Redis.")
        return {"statusCode": 404, "body": json.dumps(f"Coupon {coupon_id} not found.")}

    issued_at = coupon_info.get("issued_at")
    if not issued_at:
        logger.warning(f"Invalid coupon data for {coupon_id} in Redis.")
        return {"statusCode": 404, "body": json.dumps(f"Invalid coupon data for {coupon_id}.")}

    # Redis 쿠폰 데이터 삭제
    redis_client.delete(f"coupon:{coupon_id}")
    logger.info(f"Redis에서 쿠폰 {coupon_id} 삭제 완료")

    # Aurora MySQL에 저장
    connection = None
    try:
        connection = get_db_connection()
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT COUNT(*) FROM coupon WHERE coupon_id = %s AND member_id = %s",
                (coupon_id, member_id)
            )
            if cursor.fetchone()[0] > 0:
                logger.info(f"Coupon {coupon_id} already issued to member {member_id}.")
                return {"statusCode": 400, "body": json.dumps(f"Coupon {coupon_id} already issued.")}

            cursor.execute(
                "INSERT INTO coupon (coupon_id, member_id) VALUES (%s, %s)",
                (coupon_id, member_id)
            )
            connection.commit()
            logger.info(f"Coupon {coupon_id} successfully issued to member {member_id}.")

    except pymysql.MySQLError as e:
        logger.error(f"Aurora DB connection error: {str(e)}")
        return {"statusCode": 500, "body": json.dumps(f"Aurora DB error: {str(e)}")}

    finally:
        if connection:
            connection.close()
            logger.info("Aurora DB connection closed.")

    return {"statusCode": 200, "body": json.dumps("Coupon processed successfully.")}

# Lambda 함수 처리 (Records 기반)
def lambda_handler(event, context):
    """
    Lambda 함수 핸들러. SQS 메시지를 처리하여 Redis에 쿠폰 정보를 저장.
    """
    # Redis 클라이언트 생성
    redis_client = get_redis_client()

    # event['Records']에서 SQS 메시지 처리
    if 'Records' not in event:
        logger.error("No 'Records' found in event")
        return {
            'statusCode': 400,
            'body': json.dumps("No 'Records' found in event")
        }

    for record in event['Records']:
        # SQS 메시지의 body를 파싱
        message_body = record['body']
        logger.info(f"Received message body: {message_body}")

        try:
            # 메시지 본문을 JSON으로 파싱
            message = json.loads(message_body)

            # 필요한 데이터 추출
            coupon_id = message.get('coupon_id')
            member_id = message.get('member_id')
            timezone = message.get('timezone')
            issued_at = message.get('issued_at')
            used = message.get('used')

            # Redis에 쿠폰 데이터 저장
            coupon_data = {
                'coupon_id': coupon_id,
                'member_id': member_id,
                'timezone': timezone,
                'issued_at': issued_at,
                'used': used
            }
            
            # Redis에서 'coupon:coupon_id' 키로 저장
            redis_key = f'coupon:{coupon_id}'
            redis_client.set(redis_key, json.dumps(coupon_data))
            logger.info(f"Coupon data saved to Redis: {coupon_data}")

        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing error: {str(e)}")
        except Exception as e:
            logger.error(f"Unhandled error: {str(e)}")

    # 후속 처리: 모든 SQS 메시지 처리 후 최종 결과 반환
    return {
        'statusCode': 200,
        'body': json.dumps("All messages processed successfully.")
    }
import redis
from rediscluster import RedisCluster  # Redis 클러스터 사용

# Redis 클러스터 엔드포인트 설정
redis_host = ""
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

def initialize_coupons(redis_client):
    """쿠폰 초기화 - offline과 online 쿠폰 각각 1000개 설정"""
    initial_quantity = 1000  # 초기 쿠폰 수량

    redis_client.set("offline", initial_quantity)
    redis_client.set("online", initial_quantity)

    print(f"Initialized coupons: offline={initial_quantity}, online={initial_quantity}")

def lambda_handler(event, context):
    """Lambda 실행 시 쿠폰 개수 초기화"""
    redis_client = get_redis_client()
    initialize_coupons(redis_client)
    
    return {
        "statusCode": 200,
        "body": "Coupons initialized successfully"
    }
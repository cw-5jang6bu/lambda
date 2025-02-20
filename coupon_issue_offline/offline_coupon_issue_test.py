import unittest
from unittest.mock import MagicMock, patch
import json
import lambda_function

class TestCouponProcessor(unittest.TestCase):

    @patch('lambda_function.get_redis_client')  # get_redis_client를 모킹
    @patch('lambda_function.sqs')  # sqs 클라이언트 모킹
    def test_lambda_handler(self, mock_sqs, mock_get_redis_client):
        # Mock Redis client
        mock_redis = MagicMock()
        mock_get_redis_client.return_value = mock_redis

        # Mock SQS 클라이언트 메서드
        mock_sqs.delete_message = MagicMock()

        # SQS 메시지 예시
        sample_event = {
            "Records": [
                {
                    "receiptHandle": "some-receipt-handle",
                    "body": '{"user_id": "user123"}'
                }
            ]
        }

        # mock 메서드 설정
        mock_redis.sismember.return_value = False  # 쿠폰 미발급 상태
        mock_redis.get.return_value = "10"  # 쿠폰 잔여 개수 10개
        mock_redis.decrby.return_value = 9  # 쿠폰 차감 후 남은 수량
        mock_redis.sadd.return_value = True  # 사용자에게 쿠폰 발급 성공

        # 테스트 대상 함수 실행
        response = lambda_function.lambda_handler(sample_event, None)

        # 결과 검증
        self.assertEqual(response["statusCode"], 200)
        self.assertIn("Offline coupon granted successfully", response["body"])

        # SQS에서 메시지 삭제가 호출되었는지 확인
        mock_sqs.delete_message.assert_called_once_with(
            QueueUrl="https://sqs.ap-northeast-2.amazonaws.com/123456789012/offline-coupon-queue",
            ReceiptHandle="some-receipt-handle"
        )

if __name__ == '__main__':
    unittest.main()
import unittest
from unittest.mock import MagicMock, patch
import json
import lambda_function
import re  # 정규 표현식 추가

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
        # self.assertIn("Lambda execution completed", response["body"])

        # SQS에서 메시지 삭제가 호출되었는지 확인
        mock_sqs.delete_message.assert_called_once_with(
            QueueUrl="",
            ReceiptHandle="some-receipt-handle"
        )

        # 쿠폰 차감 후 남은 수량 검증
        mock_redis.decrby.assert_called_once_with("offline", 1)  # 'offline' 키에서 차감

        # 사용자가 쿠폰을 받았는지 확인
        mock_redis.sadd.assert_called_once_with("received_coupons", "user123")

        # 쿠폰 ID 생성 및 저장 확인 (정규 표현식 사용)
        import re

        #실제 호출된 값이 "offline-"으로 시작하는지 확인
        called_coupon_id = mock_redis.hset.call_args[0][2]  # 실제 호출된 쿠폰 ID
        assert re.match(r"offline-.*", called_coupon_id)  # 정규식으로 확인

    @patch('lambda_function.get_redis_client')  # get_redis_client를 모킹
    @patch('lambda_function.sqs')  # sqs 클라이언트 모킹
    def test_user_has_already_received_coupon(self, mock_sqs, mock_get_redis_client):
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
        mock_redis.sismember.return_value = True  # 이미 쿠폰을 받은 상태

        # 테스트 대상 함수 실행
        response = lambda_function.lambda_handler(sample_event, None)
        mock_redis.decrby.assert_not_called()

        # 결과 검증
        self.assertEqual(response["statusCode"], 400)


if __name__ == '__main__':
    unittest.main()
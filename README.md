# lambda
```plaintext
.
├── README.md
├── coupon_expired_db
│   └── lambda_fuction.py
├── coupon_init
│   ├── lambda_function.py
├── coupon_issue_offline
│   ├── lambda_function.py
│   └── offline_coupon_issue_test.py
├── coupon_issue_online
│   └── lambda_function.py
└── read_expired_coupons
    └── lambda_function.py
```
- `coupon_expired_db`: 쿠폰 정보 영속성 저장
- `coupon_init` : 쿠폰 개수 초기화
- `coupon_issue_offline` : 오프라인 쿠폰 발급
- `coupon_issue_online` : 온라인 쿠폰 발급
- `read_expired_coupons` : 만료 쿠폰 정보 이벤트 브릿지로 전달
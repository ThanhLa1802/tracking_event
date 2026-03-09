from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

# 1. Cấu hình mặc định cho các task
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 3), # Ngày bắt đầu hiệu lực
    'email_on_failure': False,          # Có thể cấu hình gửi email khi fail
    'email_on_retry': False,
    'retries': 2,                       # Nếu lỗi, thử chạy lại tối đa 2 lần
    'retry_delay': timedelta(minutes=5),# Mỗi lần thử lại cách nhau 5 phút
}

# 2. Khởi tạo DAG (Luồng công việc)
with DAG(
    dag_id='recsys_daily_batch_pipeline',
    default_args=default_args,
    description='Luồng dồn dữ liệu lịch sử từ Kafka lên MinIO mỗi đêm',
    schedule_interval='0 0 * * *',      # Biểu thức Cron: Chạy lúc 00:00 (nửa đêm) mỗi ngày
    catchup=False,
    tags=['recsys', 'data-lake'],
) as dag:

    # Task 1: Điểm bắt đầu (Chỉ mang tính chất đánh dấu)
    start_pipeline = DummyOperator(
        task_id='start_pipeline'
    )

    # Task 2: Chạy script Python gom dữ liệu lên MinIO
    # Sử dụng BashOperator để gọi lệnh trong terminal của container
    run_kafka_to_minio = BashOperator(
        task_id='export_kafka_to_minio_parquet',
        bash_command='python /opt/processing/batch/kafka_to_minio.py',
    )

    # Task 3: (Giả lập) Sau khi có dữ liệu mới trên MinIO, trigger luồng Train AI
    trigger_model_training = BashOperator(
        task_id='trigger_ai_model_retraining',
        bash_command='echo "Bắt đầu huấn luyện lại mô hình Recommendation System..."',
    )

    # Task 4: Kết thúc
    end_pipeline = DummyOperator(
        task_id='end_pipeline'
    )

    # 3. ĐIỀU PHỐI THỨ TỰ THỰC THI (Dependencies)
    # Đây là điểm ăn tiền nhất của Airflow. Dùng toán tử >> để chỉ định luồng đi.
    start_pipeline >> run_kafka_to_minio >> trigger_model_training >> end_pipeline
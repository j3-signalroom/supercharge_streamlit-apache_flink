# ScratchPad

## 1.0 Run Flink Locally
```bash
scripts/run-flink-locally.sh on --profile=AdministratorAccess-211125543747 --chip=arm64 --aws-s3-bucket=flink-kickstarter

scripts/run-flink-locally.sh off
```

### 2.0 Run Python-based Flink Apps
```bash
docker exec -it -w /opt/flink/python_apps/src supercharge_streamlit-apache_flink-jobmanager-1 /bin/bash
uv run streamlit run supercharge_streamlit/streamlit_with_local_flink.py -- --aws-s3-bucket flink_kickstarter --aws-region us-east-1
```

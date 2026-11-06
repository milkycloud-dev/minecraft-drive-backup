FROM rclone/rclone:1.71.2
RUN apk add --no-cache python3 py3-pip py3-paramiko py3-aiohttp zstd mariadb-client tzdata util-linux-misc \
 && mkdir -p /app /data && chmod 777 /data
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --break-system-packages -r requirements.txt
COPY drivebackup ./drivebackup
ENTRYPOINT ["python3", "-m", "drivebackup.panel"]

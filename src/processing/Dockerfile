FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

LABEL maintainer="data-engineering@nexusflow.io" \
      project="nexusflow" \
      service="processing"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["spark-submit"]
CMD ["bronze_to_silver.py"]

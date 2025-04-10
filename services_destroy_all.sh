#!/bin/bash
set -e

# Load variables from .env
set -a
source .env
set +a

echo "🌍 Setting environment and Kafka cluster..."
confluent environment use "$ENV_ID"
confluent kafka cluster use "$CLUSTER_ID"

# ----------------------------------------------------------------
# 1) DELETE SCHEMA (Subject)
# ----------------------------------------------------------------
echo
echo "🧹 Checking for Schema subject '$CC_TOPIC_NAME-value'..."
SUBJECT_EXISTS=$(confluent schema-registry schema get --subject "$CC_TOPIC_NAME-value" --version latest --output json 2>/dev/null || true)
if [ -z "$SUBJECT_EXISTS" ]; then
  echo "ℹ️ Schema subject '$CC_TOPIC_NAME-value' not found, skipping..."
else
  echo "  🗑️ Deleting schema subject: $CC_TOPIC_NAME-value"
  yes y | confluent schema-registry schema delete --subject "$CC_TOPIC_NAME-value"
  echo "✅ Schema subject '$CC_TOPIC_NAME-value' deleted."
fi

# ----------------------------------------------------------------
# 2) DELETE TOPIC
# ----------------------------------------------------------------
echo
echo "🧹 Checking for topic '$CC_TOPIC_NAME'..."
TOPIC_EXISTS=$(confluent kafka topic describe "$CC_TOPIC_NAME" --output json 2>/dev/null || true)
if [ -z "$TOPIC_EXISTS" ]; then
  echo "ℹ️ Topic '$CC_TOPIC_NAME' not found, skipping..."
else
  echo "  🗑️ Deleting topic: $CC_TOPIC_NAME"
  yes y | confluent kafka topic delete "$CC_TOPIC_NAME"
  echo "✅ Topic '$CC_TOPIC_NAME' deleted."
fi

echo "✅ Reset complete."

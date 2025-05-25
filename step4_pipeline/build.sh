#!/bin/bash

set -e

LAMBDA_DIR="lambda"
BUILD_DIR="build"
BUCKET="code-bucket"
STACK_NAME="rearc-data-pipeline"

mkdir -p $BUILD_DIR

# Function to package lambda with dependencies
package_lambda() {
  FUNC_NAME=$1
  cd $LAMBDA_DIR

  mkdir -p ../$BUILD_DIR/$FUNC_NAME
  cp $FUNC_NAME.py ../$BUILD_DIR/$FUNC_NAME/

  cd ../$BUILD_DIR/$FUNC_NAME
  python3 -m pip install requests bs4 pandas -t . >/dev/null
  zip -r ../$FUNC_NAME.zip . >/dev/null

  cd ../../
}

echo "Packaging ingest.py..."
package_lambda ingest
package_lambda analytics


echo "Uploading to S3..."
awslocal s3 mb s3://$BUCKET --endpoint-url=http://localhost:4566 || true
awslocal s3 cp $BUILD_DIR/ingest.zip s3://$BUCKET/ingest.zip --endpoint-url=http://localhost:4566
awslocal s3 cp $BUILD_DIR/analytics.zip s3://$BUCKET/analytics.zip --endpoint-url=http://localhost:4566

echo "Deploying CloudFormation Stack..."
awslocal cloudformation deploy \
  --template-file template.yaml \
  --stack-name $STACK_NAME \
  --capabilities CAPABILITY_NAMED_IAM \
  --endpoint-url=http://localhost:4566

echo "✅ Deployment Complete!"

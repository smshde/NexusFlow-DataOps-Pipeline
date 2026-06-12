#!/usr/bin/env bash
# NexusFlow — Post-destroy cleanup + cost verification
# Run after terraform destroy

REGION="ca-central-1"

echo "=== 1. EMR Serverless apps ==="
for APP in $(aws emr-serverless list-applications --region $REGION --query 'applications[*].applicationId' --output text); do
  aws emr-serverless stop-application --application-id $APP --region $REGION 2>/dev/null
  sleep 10
  aws emr-serverless delete-application --application-id $APP --region $REGION 2>/dev/null && echo "deleted EMR $APP"
done

echo "=== 2. ECR images ==="
for REPO in datagen ingestion serving dbt processing dashboard; do
  IMAGES=$(aws ecr list-images --repository-name nexusflow-$REPO --region $REGION --query 'imageIds' --output json 2>/dev/null)
  [ "$IMAGES" != "[]" ] && [ -n "$IMAGES" ] && aws ecr batch-delete-image --repository-name nexusflow-$REPO --image-ids "$IMAGES" --region $REGION 2>/dev/null && echo "cleared $REPO"
done

echo "=== 3. CloudWatch log groups ==="
for LG in $(aws logs describe-log-groups --query 'logGroups[?contains(logGroupName,`nexusflow`)].logGroupName' --output text --region $REGION); do
  aws logs delete-log-group --log-group-name "$LG" --region $REGION && echo "deleted $LG"
done

echo "=== 4. NAT Gateways ==="
aws ec2 describe-nat-gateways --filter "Name=tag:Project,Values=nexusflow" "Name=state,Values=available" --query 'NatGateways[*].NatGatewayId' --output text --region $REGION | tr '\t' '\n' | while read NG; do
  [ -n "$NG" ] && aws ec2 delete-nat-gateway --nat-gateway-id $NG --region $REGION && echo "deleted NAT $NG"
done

echo "=== 5. Elastic IPs ==="
for ALLOC in $(aws ec2 describe-addresses --filters "Name=tag:Project,Values=nexusflow" --query 'Addresses[*].AllocationId' --output text --region $REGION); do
  aws ec2 release-address --allocation-id $ALLOC --region $REGION && echo "released EIP $ALLOC"
done

echo "=== 6. Load Balancers (orphaned) ==="
for LB in $(aws elbv2 describe-load-balancers --query "LoadBalancers[?contains(LoadBalancerName,'nexusflow') || contains(LoadBalancerName,'k8s')].LoadBalancerArn" --output text --region $REGION); do
  aws elbv2 delete-load-balancer --load-balancer-arn $LB --region $REGION && echo "deleted LB $LB"
done

echo "=== 7. Verify zero compute/networking resources ==="
aws ec2 describe-instances --filters "Name=tag:Project,Values=nexusflow" "Name=instance-state-name,Values=running" --query 'Reservations[*].Instances[*].InstanceId' --output text --region $REGION

echo "=== 8. Yesterday + Today actual cost ==="
aws ce get-cost-and-usage \
  --time-period Start=$(date -v-2d +%Y-%m-%d),End=$(date +%Y-%m-%d) \
  --granularity DAILY \
  --metrics "UnblendedCost" \
  --group-by Type=DIMENSION,Key=SERVICE \
  --output table --region us-east-1

echo "=== CLEANUP COMPLETE ==="
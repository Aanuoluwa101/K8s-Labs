# 1. Create the topic (RF 3, min ISR 2)
kubectl -n kafka-lab exec kafka-0 -- kafka-topics --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 3 --replication-factor 3 --config min.insync.replicas=2

# 2. Ship the scripts into the cluster as a ConfigMap
kubectl -n kafka-lab create configmap kafka-clients --from-file=producer.py --from-file=consumer.py

# 3. Start the producer (1 pod) and consumer group (2 pods)
kubectl apply -f clients.yaml

# 4. Watch
kubectl -n kafka-lab logs -f deploy/producer
kubectl -n kafka-lab logs -f -l app=consumer --prefix
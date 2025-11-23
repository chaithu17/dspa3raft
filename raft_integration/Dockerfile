FROM python:3.9-slim

WORKDIR /app

# Copy proto files (now local)
COPY raft.proto ./
COPY raft_pb2.py ./
COPY raft_pb2_grpc.py ./

COPY polling.proto ./
COPY polling_pb2.py ./
COPY polling_pb2_grpc.py ./

# Copy integrated node
COPY raft_polling_node.py ./

# Install dependencies
RUN pip install --no-cache-dir grpcio grpcio-tools protobuf

# Expose port
EXPOSE 50051

# Run the node (will be overridden by docker-compose)
CMD ["python", "raft_polling_node.py", "1"]

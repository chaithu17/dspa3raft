FROM python:3.9-slim

WORKDIR /app

# Copy Raft proto files and generated code
COPY ../raft_implementation/raft.proto ./
COPY ../raft_implementation/raft_pb2.py ./
COPY ../raft_implementation/raft_pb2_grpc.py ./

# Copy Polling proto files and generated code
COPY ../microservice_rpc/app/polling.proto ./
COPY ../microservice_rpc/app/polling_pb2.py ./
COPY ../microservice_rpc/app/polling_pb2_grpc.py ./

# Copy integrated node
COPY raft_polling_node.py ./

# Install dependencies
RUN pip install --no-cache-dir grpcio grpcio-tools protobuf

# Expose port
EXPOSE 50051

# Run the node
CMD ["python", "raft_polling_node.py"]

#!/bin/bash
echo "Setting up Raft integration..."

# Copy Raft proto files
cp ../raft_implementation/raft.proto ./
cp ../raft_implementation/raft_pb2.py ./
cp ../raft_implementation/raft_pb2_grpc.py ./

# Copy Polling proto files
cp ../microservice_rpc/app/polling.proto ./
cp ../microservice_rpc/app/polling_pb2.py ./
cp ../microservice_rpc/app/polling_pb2_grpc.py ./

echo "✓ Files copied successfully!"
ls -la *.proto *.py

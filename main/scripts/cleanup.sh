#!/bin/bash
echo "Deleting all resources in namespace course-reg-exp..."
kubectl delete ns course-reg-exp --ignore-not-found
echo "Cleaning up k6 operator resources..."
kubectl delete testrun --all -n course-reg-exp --ignore-not-found
kubectl delete configmap k6-script -n course-reg-exp --ignore-not-found
echo "Cleanup complete."